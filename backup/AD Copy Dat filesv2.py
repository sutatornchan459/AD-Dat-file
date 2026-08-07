import os
import shutil
import glob
import pandas as pd
import socket
from sqlalchemy import create_engine, text

def clean_and_create_folder(path_save):
    # ลบโฟลเดอร์หากมีอยู่แล้ว แล้วสร้างใหม่
    if os.path.isdir(path_save):
        shutil.rmtree(path_save, ignore_errors=True)
    os.mkdir(path_save)

name_pc = socket.gethostname()
factory = 'A' if name_pc == "2592P-ED363" else 'E'

engine = create_engine("mysql+pymysql://vip:123456@172.18.106.100/vipdatabase")
with engine.connect() as con:
    ad_ip = pd.read_sql_query(text(f"SELECT * FROM ad_ip WHERE Factory = '{factory}'"), con)

for item in ad_ip.itertuples():
    mc = item.Machine
    ip = rf"\\{item.IP}\{item.Version}\Dat"
    if not os.path.isdir(ip):
        ip = rf"\\{item.IP}\{item.Machine}\{item.Version}\Dat"
    if os.path.isdir(ip):
        check_file = glob.glob(rf"{ip}\*.dat")
        if check_file:
            print(mc, ': total', len(check_file), 'files')
            path_save = os.path.join(r"\\172.18.106.224\mc_csv6\QC\logad", mc)
            clean_and_create_folder(path_save)

            for file in check_file:
                name = os.path.basename(file)  # เปลี่ยนเป็น basename เพื่อรองรับ path ทุกแบบ
                if name.lower().endswith('.dat'):  # รองรับ .dat/.Dat ทุกเคส
                    shutil.copy(file, os.path.join(path_save, name))
        else:
            print(mc, ': No .dat files found')
    else:
        print(mc, "Cannot Connect")
