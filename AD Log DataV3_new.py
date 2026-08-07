import pandas as pd
import os
import socket
import time
import logging
import re
from datetime import datetime as dt, timedelta as td
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
import numpy as np
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
NETWORK_TIMEOUT_SEC = 5     # timeout ตรวจสอบ network path
FILE_TIMEOUT_SEC    = 10    # timeout อ่านไฟล์
DB_BATCH_SIZE       = 100   # insert ทีละกี่แถว
DB_RETRY            = 3     # จำนวนครั้ง retry เมื่อ DB error
DAYS_BACK           = 180   # ย้อนหลังกี่วัน
DAT_NAME_MAX_LEN    = 100   # จำกัดความยาว dat_name ป้องกัน Data too long
CAS_FIELD_MAX_LEN   = 255   # จำกัดความยาว alarm/Status ป้องกัน MySQL 1406 Data too long
                            # ตรวจค่าจริงด้วย: SHOW CREATE TABLE inadatabase.ad_log3;

# เครื่องที่ไม่ต้องประมวลผล — ใส่ชื่อเครื่องตามที่อยู่ในคอลัมน์ Machine ของตาราง ad_ip
# AD05: เชื่อมต่อ path ไม่ได้ 22 ครั้งจาก 12 รอบที่รัน (7 เม.ย.–23 ก.ค. 2026) ตัดออกเมื่อ 2026-08-08
EXCLUDE_MACHINES = {"AD05"}

# ชื่อไฟล์ที่ต้องการประมวลผล
TARGET_FILE_PATTERNS = ["_2G.log", "_2G.Log", "_2G.txt", "WorkStatus.log", "WorkStatus.Log", "WorkStatus.txt"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("ad_log2_run.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Helper: network & file operations
# ─────────────────────────────────────────────
def safe_isdir(path: str, timeout: float = NETWORK_TIMEOUT_SEC) -> bool:
    with ThreadPoolExecutor(max_workers=1) as ex:
        future = ex.submit(os.path.isdir, path)
        try: return future.result(timeout=timeout)
        except (FuturesTimeoutError, Exception): return False

def safe_listdir(path: str, timeout: float = NETWORK_TIMEOUT_SEC) -> list:
    with ThreadPoolExecutor(max_workers=1) as ex:
        future = ex.submit(os.listdir, path)
        try: return future.result(timeout=timeout)
        except (FuturesTimeoutError, Exception): return []

def safe_readlines(path: str, timeout: float = FILE_TIMEOUT_SEC) -> list:
    def _read():
        with open(path, encoding="latin-1") as f: return f.readlines()
    with ThreadPoolExecutor(max_workers=1) as ex:
        future = ex.submit(_read)
        try: return future.result(timeout=timeout)
        except (FuturesTimeoutError, Exception) as e:
            log.warning(f"อ่านไฟล์ timeout/error: {path} → {e}")
            return []

# ─────────────────────────────────────────────
# Helper: Database engine & Insert
# ─────────────────────────────────────────────
def make_engine(db_name: str):
    return create_engine(
        f"mysql+pymysql://vip:123456@172.18.106.100/{db_name}?connect_timeout=10",
        poolclass=QueuePool,
        pool_size=3,
        max_overflow=2,
        pool_recycle=3600,
        pool_pre_ping=True,
        echo=False,
    )

def insert_rows_individually(batch: pd.DataFrame, engine, table: str) -> int:
    """batch ล้มเหลว → insert ทีละแถว เพื่อไม่ให้แถวเสียแถวเดียวทำให้ทั้ง batch หายไป"""
    ok = 0
    for idx in range(len(batch)):
        row = batch.iloc[[idx]]
        try:
            with engine.connect() as con:
                row.to_sql(table, con, if_exists="append", index=False)
            ok += 1
        except Exception as e:
            r = row.iloc[0]
            log.error(f"ข้ามแถว id={r.get('id', '?')} machine={r.get('machine', '?')}: {e}")
    return ok

def insert_with_retry(df: pd.DataFrame, engine, table: str, batch_size: int = DB_BATCH_SIZE, retries: int = DB_RETRY):
    total = len(df)
    inserted = 0
    for start in range(0, total, batch_size):
        batch = df.iloc[start: start + batch_size]
        for attempt in range(1, retries + 1):
            try:
                with engine.connect() as con:
                    batch.to_sql(table, con, if_exists="append", index=False)
                inserted += len(batch)
                break
            except Exception as e:
                log.warning(f"Insert batch {start}–{start+len(batch)} attempt {attempt}/{retries}: {e}")
                if attempt == retries:
                    log.warning(f"batch {start} ล้มเหลว {retries} ครั้ง — เปลี่ยนเป็น insert ทีละแถว")
                    ok = insert_rows_individually(batch, engine, table)
                    inserted += ok
                    if ok < len(batch):
                        log.error(f"ข้ามไป {len(batch) - ok} แถว (batch {start}) เนื่องจาก error ซ้ำ")
                else:
                    time.sleep(2 ** attempt)
    log.info(f"Insert สำเร็จ {inserted}/{total} แถว → {table}")

# ─────────────────────────────────────────────
# Logic: สกัดวันที่จาก Path
# ─────────────────────────────────────────────
def extract_date_from_path(full_path: str) -> str:
    """ ดึงตัวเลข 8 หลัก (20xxxxxx) จาก path เพื่อใช้เป็นวันที่ฐาน """
    match = re.search(r'(20\d{6})', full_path)
    return match.group(1) if match else None

# ─────────────────────────────────────────────
# Field mapping & Parsing
# ─────────────────────────────────────────────
FIELD_MAP = {
    "=ProductName=":    "product",       "=ProductName=,":    "product",
    "=PrdtName=":       "product",       "=PrdtName=,":       "product",
    "=DatName=":        "dat_name",      "=DatName=,":        "dat_name",
    "=DataFile=":       "dat_name",      "=DataFile=,":       "dat_name",
    "=LotN=":           "LotName",       "=LotN=,":           "LotName",
    "=LotNo=":          "LotName",       "=LotNo=,":          "LotName",
    "=StTime=":         "start_datetime","=StTime=,":         "start_datetime",
    "=StartDate=":      "start_datetime","=StartDate=,":      "start_datetime",
    "=EdTime=":         "finish_datetime","=EdTime=,":        "finish_datetime",
    "=EndDate=":        "finish_datetime","=EndDate=,":       "finish_datetime",
    "=LaserPwr=":       "laser_power",   "=LaserPwr=,":       "laser_power",
    "=OrgPwrDp=":       "laser_power",   "=OrgPwrDp=,":       "laser_power",
    "=Touka=":          "transmittance", "=Touka=,":          "transmittance",
    "=TransmitDp=":     "transmittance", "=TransmitDp=,":     "transmittance",
    "=MaxPwr=":         "max_power",     "=MaxPwr=,":         "max_power",
    "=PeakPwrDp=":      "max_power",     "=PeakPwrDp=,":      "max_power",
    "=Workpwr=":        "work_power",    "=Workpwr=,":        "work_power",
    "=WorkPwrDp=":      "work_power",    "=WorkPwrDp=,":      "work_power",
    "=WkZ=":            "laser_position","=WkZ=,":            "laser_position",
    "=DataThkPerDp=":   "laser_position","=DataThkPerDp=,":   "laser_position",
    "=SdePp=":          "pp_patern",     "=SdePp=,":          "pp_patern",
    "=SdeBe=":          "be_pattern",    "=SdeBe=,":          "be_pattern",
    "=SdeBw=":          "bw_pattern",    "=SdeBw=,":          "bw_pattern",
    "=TactAll=":        "total_time",    "=TactAll=,":        "total_time",
    "=TotalTakt=":      "total_time",    "=TotalTakt=,":      "total_time",
}

WAFER_KEYS = {"=WaferParameter=,", "=WaferParameter="}
CAS_KEYS   = {"=CasSts=,", "=CasSts=", "=RecProps=", "=RecProps=,"}

# บรรทัดหัวข้อ section เช่น "=CasSts=," หรือ "=WaferParameter="
SECTION_HEADER_RE = re.compile(r"^=\w+=,?$")

def section_rows(log_data: list, start: int) -> list:
    """อ่านแถวของ section ตั้งแต่ start จนถึงหัวข้อ section ถัดไป — ไม่ใช่จนจบไฟล์
    (การอ่านจนจบไฟล์คือต้นเหตุที่ทำให้ alarm/Status ยาวเกินคอลัมน์)"""
    rows = []
    for line2 in log_data[start:]:
        if SECTION_HEADER_RE.match(line2.split(" ")[0].strip()):
            break
        rows.append([x.strip() for x in line2.split(",")])
    return rows

def capped(values, field: str) -> str:
    """join แล้วจำกัดความยาว กัน MySQL 1406 — และ log ให้เห็นเมื่อถูกตัด ไม่ใช่หายเงียบ"""
    joined = ", ".join(values)
    if len(joined) > CAS_FIELD_MAX_LEN:
        log.warning(f"ตัด {field} จาก {len(joined)} เหลือ {CAS_FIELD_MAX_LEN} ตัวอักษร")
        return joined[:CAS_FIELD_MAX_LEN]
    return joined

def parse_log(log_data: list, path_date_str: str) -> dict:
    data = {}
    for row, line in enumerate(log_data):
        name = line.split(" ")[0].strip()
        if not name:
            continue

        if name in FIELD_MAP:
            field = FIELD_MAP[name]
            val = log_data[row + 1].strip() if row + 1 < len(log_data) else ""

            if field == "dat_name":
                val = val.split(".")[0][:DAT_NAME_MAX_LEN]
            data[field] = val

        elif name in WAFER_KEYS:
            rows = section_rows(log_data, row + 2)
            if rows:
                df = pd.DataFrame(rows)
                if df.shape[1] > 3:
                    col_data = pd.to_numeric(df.iloc[:, 3], errors="coerce")
                    col_data = col_data.replace([999, 9999, 99999], np.nan)
                    data["wafer_thickness"] = str(round(col_data.mean(skipna=True), 2))

        elif name in CAS_KEYS:
            rows = section_rows(log_data, row + 1)
            if rows:
                df = pd.DataFrame(rows)
                if df.shape[1] >= 3:
                    df2 = df.iloc[:, 1:3].copy()
                    df2.columns = ["alarm", "Status"]
                    df_121 = df2[df2["alarm"] == "121"]; df_225 = df2[df2["alarm"] == "225"]
                    data["alarm"] = capped(df_121["alarm"].astype(str), "alarm")
                    data["Status"] = capped(df_121["Status"].astype(str), "Status")
                    data["alarm225"] = capped(df_225["alarm"].astype(str), "alarm225")
                    data["Status225"] = capped(df_225["Status"].astype(str), "Status225")

    # ประกบวันที่จาก path เข้ากับเวลาในไฟล์ พร้อมเช็คการข้ามคืน
    # (ต้องทำหลังอ่านครบทั้งสองค่า ไม่งั้นเทียบ start/finish ไม่ได้)
    if path_date_str and "start_datetime" in data and "finish_datetime" in data:
        try:
            base_dt = dt.strptime(path_date_str, "%Y%m%d")

            st_time = pd.to_datetime(data["start_datetime"].split(" ")[-1], errors="coerce").time()
            ed_time = pd.to_datetime(data["finish_datetime"].split(" ")[-1], errors="coerce").time()

            if st_time and ed_time:
                start_full = dt.combine(base_dt.date(), st_time)
                finish_full = dt.combine(base_dt.date(), ed_time)

                # ถ้าเวลาจบน้อยกว่าเวลาเริ่ม แสดงว่างานข้ามไปอีกวัน
                if finish_full < start_full:
                    finish_full = finish_full + td(days=1)

                data["start_datetime"] = start_full
                data["finish_datetime"] = finish_full
        except Exception as e:
            log.warning(f"Datetime parse error: {e}")

    return data

# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
def main():
    name_pc = socket.gethostname()
    factory = "A" if name_pc == "2592P-ED363" else "E"
    log.info(f"เริ่มทำงาน | PC: {name_pc} | Factory: {factory}")

    src_engine = make_engine("vipdatabase")
    with src_engine.connect() as con:
        ad_ip = pd.read_sql_query(text("SELECT * FROM ad_ip WHERE Factory = :f"), con, params={"f": factory})

    all_rows = []
    for item in ad_ip.itertuples():
        mc = item.Machine
        if mc in EXCLUDE_MACHINES:
            log.info(f"[{mc}] อยู่ใน EXCLUDE_MACHINES — ข้ามเครื่องนี้")
            continue
        candidates = [
            rf"\\{item.IP}\{item.Version}\_Log\Auto",
            rf"\\{item.IP}\{item.Version}\Auto",
            rf"\\{item.IP}\{item.Machine}\{item.Version}\Auto",
            rf"\\{item.IP}\{item.Machine}\Dat",
        ]
        path = next((p for p in candidates if safe_isdir(p)), None)
        if path is None: 
            log.warning(f"[{mc}] ไม่พบ path ที่เข้าถึงได้ — ข้ามเครื่องนี้")
            continue

        log.info(f"[{mc}] path: {path}")
        for i in range(DAYS_BACK, -1, -1):
            date_folder = (dt.now() - td(days=i)).strftime("%Y%m%d")
            path_date = os.path.join(path, date_folder)
            if not safe_isdir(path_date): continue

            for file in safe_listdir(path_date):
                if not any(pat in file for pat in TARGET_FILE_PATTERNS): continue
                
                path_file = os.path.join(path_date, file)
                # สกัดวันที่จาก path (ส่งไปให้ parse_log)
                path_date_str = extract_date_from_path(path_file)
                
                log_data = safe_readlines(path_file)
                if not log_data: continue

                data = parse_log(log_data, path_date_str)
                if not data: continue

                data["id"] = file.split(".")[0]
                data["machine"] = mc
                all_rows.append(data)

        log.info(f"[{mc}] รวม {len(all_rows)} แถวสะสม")

    if not all_rows:
        log.info("ไม่มีข้อมูลใหม่")
        return

    all_data = pd.DataFrame(all_rows)
    
    STR_COLS = {"machine", "id", "product", "dat_name", "pp_patern", "be_pattern", "bw_pattern", "alarm", "Status", "alarm225", "Status225", "LotName", "wafer_thickness"}
    
    for col in all_data.columns:
        if col in ("start_datetime", "finish_datetime"):
            all_data[col] = pd.to_datetime(all_data[col], errors="coerce")
        elif col not in STR_COLS:
            all_data[col] = pd.to_numeric(all_data[col], errors="coerce")

    all_data.sort_values("start_datetime", inplace=True, ignore_index=True)
    
    dst_engine = make_engine("inadatabase")
    insert_with_retry(all_data, dst_engine, "ad_log3")
    log.info("เสร็จสิ้น")

if __name__ == "__main__":
    main()