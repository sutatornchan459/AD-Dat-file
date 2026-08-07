import pandas as pd
import os
import socket
import time
import logging
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
DAYS_BACK           = 1  # ย้อนหลังกี่วัน
DAT_NAME_MAX_LEN    = 100   # จำกัดความยาว dat_name ป้องกัน Data too long

# ชื่อไฟล์ที่ต้องการประมวลผล
TARGET_FILE_PATTERNS = ["_2G.log", "_2G.Log", "_2G.txt", "WorkStatus.log", "WorkStatus.Log", "WorkStatus.txt",".log",".Log",".txt"]

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
# Helper: network operations พร้อม timeout
# ─────────────────────────────────────────────
def safe_isdir(path: str, timeout: float = NETWORK_TIMEOUT_SEC) -> bool:
    with ThreadPoolExecutor(max_workers=1) as ex:
        future = ex.submit(os.path.isdir, path)
        try:
            return future.result(timeout=timeout)
        except (FuturesTimeoutError, Exception):
            return False


def safe_listdir(path: str, timeout: float = NETWORK_TIMEOUT_SEC) -> list:
    with ThreadPoolExecutor(max_workers=1) as ex:
        future = ex.submit(os.listdir, path)
        try:
            return future.result(timeout=timeout)
        except (FuturesTimeoutError, Exception):
            return []


def safe_readlines(path: str, timeout: float = FILE_TIMEOUT_SEC) -> list:
    def _read():
        with open(path, encoding="latin-1") as f:
            return f.readlines()

    with ThreadPoolExecutor(max_workers=1) as ex:
        future = ex.submit(_read)
        try:
            return future.result(timeout=timeout)
        except (FuturesTimeoutError, Exception) as e:
            log.warning(f"อ่านไฟล์ timeout/error: {path} → {e}")
            return []


# ─────────────────────────────────────────────
# Helper: สร้าง engine พร้อม pool & timeout
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


# ─────────────────────────────────────────────
# Helper: batch insert พร้อม retry
# ─────────────────────────────────────────────
def insert_with_retry(df: pd.DataFrame, engine, table: str,
                      batch_size: int = DB_BATCH_SIZE, retries: int = DB_RETRY):
    total    = len(df)
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
                    log.error(f"ข้ามไป {len(batch)} แถว (batch {start}) เนื่องจาก error ซ้ำ")
                else:
                    time.sleep(2 ** attempt)
    log.info(f"Insert สำเร็จ {inserted}/{total} แถว → {table}")


# ─────────────────────────────────────────────
# Field mapping
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


# ─────────────────────────────────────────────
# Parse log file → dict
# ─────────────────────────────────────────────
def parse_log(log_data: list) -> dict:
    data = {}
    for row, line in enumerate(log_data):
        name = line.split(" ")[0].strip()
        if not name:
            continue

        # ── fields ธรรมดา ──────────────────────
        if name in FIELD_MAP:
            field = FIELD_MAP[name]
            val = log_data[row + 1].strip() if row + 1 < len(log_data) else ""
            if field == "dat_name":
                # ตัด extension และจำกัดความยาว ป้องกัน "Data too long"
                val = val.split(".")[0][:DAT_NAME_MAX_LEN]
            data[field] = val

        # ── Wafer Thickness ────────────────────
        elif name in WAFER_KEYS:
            rows = []
            for line2 in log_data[row + 2:]:
                cols = [x.strip() for x in line2.split(",")]
                rows.append(cols)
            if rows:
                df = pd.DataFrame(rows)
                if df.shape[1] > 3:
                    col_data = pd.to_numeric(df.iloc[:, 3], errors="coerce")
                    col_data = col_data.replace([999, 9999, 99999], np.nan)
                    data["wafer_thickness"] = str(round(col_data.mean(skipna=True), 2))

        # ── Alarm & Status ─────────────────────
        elif name in CAS_KEYS:
            rows = []
            for line2 in log_data[row + 1:]:
                cols = [x.strip() for x in line2.split(",")]
                rows.append(cols)
            if rows:
                df = pd.DataFrame(rows)
                if df.shape[1] >= 3:
                    df2 = df.iloc[:, 1:3].copy()
                    df2.columns = ["alarm", "Status"]
                    df_121 = df2[df2["alarm"] == "121"]
                    df_225 = df2[df2["alarm"] == "225"]
                    data["alarm"]     = ", ".join(df_121["alarm"].astype(str))
                    data["Status"]    = ", ".join(df_121["Status"].astype(str))
                    data["alarm225"]  = ", ".join(df_225["alarm"].astype(str))
                    data["Status225"] = ", ".join(df_225["Status"].astype(str))
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
        ad_ip = pd.read_sql_query(
            text("SELECT * FROM ad_ip WHERE Factory = :f"),
            con, params={"f": factory},
        )

    all_rows = []

    for item in ad_ip.itertuples():
        mc = item.Machine

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

        for i in range(DAYS_BACK, 0, -1):
            date      = (dt.now() - td(days=i)).strftime("%Y%m%d")
            path_date = os.path.join(path, date)
            if not safe_isdir(path_date):
                continue

            for file in safe_listdir(path_date):
                if not any(pat in file for pat in TARGET_FILE_PATTERNS):
                    continue

                path_file = os.path.join(path_date, file)
                log_data  = safe_readlines(path_file)
                if not log_data:
                    continue

                data = parse_log(log_data)
                if not data:
                    continue

                data["id"]      = file.split(".")[0]
                data["machine"] = mc
                all_rows.append(data)

        log.info(f"[{mc}] รวม {len(all_rows)} แถวสะสม")

    if not all_rows:
        log.info("ไม่มีข้อมูลใหม่")
        return

    all_data = pd.DataFrame(all_rows)

    # ── แปลง dtype ──────────────────────────────
    STR_COLS = {
        "machine", "id", "product", "dat_name",
        "pp_patern", "be_pattern", "bw_pattern",
        "alarm", "Status", "alarm225", "Status225",
        "LotName", "wafer_thickness",
    }
    for col in all_data.columns:
        if col in ("start_datetime", "finish_datetime"):
            # ส่งเป็น datetime object → SQLAlchemy แปลงเป็น DATETIME ให้อัตโนมัติ
            # MySQL จะเก็บในรูป yyyy-mm-dd hh:mm:ss
            all_data[col] = pd.to_datetime(all_data[col], format="mixed", errors="coerce")
        elif col not in STR_COLS:
            all_data[col] = pd.to_numeric(all_data[col], errors="coerce")

    all_data.sort_values("start_datetime", inplace=True, ignore_index=True)
    log.info(f"DataFrame พร้อม: {all_data.shape[0]} แถว × {all_data.shape[1]} คอลัมน์")

    dst_engine = make_engine("inadatabase")
    insert_with_retry(all_data, dst_engine, "ad_log3")
    log.info("เสร็จสิ้น")


if __name__ == "__main__":
    main()