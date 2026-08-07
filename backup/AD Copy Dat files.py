import os
import shutil
import glob
import pandas as pd
import socket
from sqlalchemy import create_engine, text

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
    ip = rf"\\{item.IP}\{item.Version}\Dat"
    if os.path.isdir(ip) == False:
        ip = rf"\\{item.IP}\{item.Machine}\{item.Version}\Dat"
    if os.path.isdir(ip) == True:
        check_file = glob.glob(rf"{ip}\*.dat")
        if len(check_file) != 0:
            print(mc, ': total', len(check_file), 'files')
            path_save = os.path.join(r"\\172.18.106.224\mc_csv6\QC\logad", mc)
            if os.path.isdir(path_save) == True:
                shutil.rmtree(path_save, ignore_errors=True)
                os.mkdir(path_save)
            else:
                os.mkdir(path_save)
            for file in check_file:
                name = file.split('\\')[5]
                if '.Dat' in name:
                    shutil.copy(file, os.path.join(path_save, name))
    else:
        print(mc, "Cannot Connect")