import os
import shutil
import glob
import pandas as pd
import socket
from sqlalchemy import create_engine, text

# เครื่องที่ไม่ต้องประมวลผล — ใส่ชื่อเครื่องตามที่อยู่ในคอลัมน์ Machine ของตาราง ad_ip
# AD05: เชื่อมต่อ path ไม่ได้ 22 ครั้งจาก 12 รอบที่รัน (7 เม.ย.–23 ก.ค. 2026) ตัดออกเมื่อ 2026-08-08
EXCLUDE_MACHINES = {"AD05"}

name_pc = socket.gethostname()
if name_pc == "2592P-ED363":
    factory = 'A'
else:
    factory = 'E'

engine = create_engine("mysql+pymysql://vip:123456@172.18.106.100/vipdatabase")
with engine.connect() as con:
    ad_ip = pd.read_sql_query(text(f"SELECT * FROM ad_ip WHERE Factory = '{factory}'"), con)

for item in ad_ip.itertuples():
    mc = item.Machine
    if mc in EXCLUDE_MACHINES:
        print(mc, ": อยู่ใน EXCLUDE_MACHINES — ข้าม")
        continue
    try:
        ip = rf"\\{item.IP}\{item.Version}\Auto"
        if os.path.isdir(ip) == False:
            ip = rf"\\{item.IP}\{item.Machine}\{item.Version}\Auto"
        if os.path.isdir(ip) == True:
            check_file = glob.glob(rf"{ip}\*.log")
            if len(check_file) == 0:
                check_file = glob.glob(rf"{ip}\*.Log")
            if len(check_file) != 0:
                print(mc, ': total', len(check_file), 'files')
                path_save = os.path.join(r"\\172.18.106.55\FileTransfer_IT-OT\DI\addata\auto", mc)
                if os.path.isdir(path_save) == True:
                    shutil.rmtree(path_save, ignore_errors=True)
                os.makedirs(path_save, exist_ok=True)
                for file in check_file:
                    name = os.path.basename(file)
                    if '.log' in name:
                        shutil.copy(file, os.path.join(path_save, name))
                    elif '.Log' in name:
                        shutil.copy(file, os.path.join(path_save, name))
        else:
            print(mc, "Cannot Connect")
    except Exception as e:
        # เครื่องเดียวพังต้องไม่ทำให้เครื่องที่เหลือในรอบนี้ไม่ถูกประมวลผล
        print(mc, "ERROR:", e)
        continue