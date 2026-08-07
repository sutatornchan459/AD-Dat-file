import pandas as pd
import os
import socket
from datetime import datetime as dt, timedelta as td
from sqlalchemy import create_engine, text

name_pc = socket.gethostname()
if name_pc == "2592P-ED363":
    factory = 'A'
else:
    factory = 'E'

engine = create_engine("mysql+pymysql://vip:123456@172.18.106.100/vipdatabase")
with engine.connect() as con:
    ad_ip = pd.read_sql_query(text(f"SELECT * FROM ad_ip WHERE Factory = '{factory}'"), con)

all_data = pd.DataFrame()
for item in ad_ip.itertuples():
    mc = 8
    path = rf"\\{item.IP}\{item.Version}\_Log\Auto"
    if os.path.isdir(path) == False:
        path = rf"\\{item.IP}\{item.Version}\Auto"
        if os.path.isdir(path) == False:
            path = rf"\\{item.IP}\{item.Machine}\{item.Version}\Auto"
    if os.path.isdir(path) == True:
        for i in range(30,0,-1):
            date = (dt.now()-td(days=i)).strftime("%Y%m%d")
            path_date = os.path.join(path, date)
            if os.path.isdir(path_date):
                for file in os.listdir(path_date):
                    if ".log" in file or ".Log" in file or ".txt" in file:
                        path_file = os.path.join(path_date, file)
                        data = {}
                        with open(path_file, encoding="latin-1") as f:
                            log_data = f.readlines()
                        for row in range(len(log_data)):
                            name = log_data[row].split(" ")[0].strip()
                            if len(name) > 0:
                                if name in ["=ProductName=", "=PrdtName="]:
                                    data["product"] = [log_data[row+1].strip()]
                                elif name in ["=DatName=", "=DataFile="]:
                                    data["dat_name"] = [log_data[row+1].strip().split(".")[0]]
                                elif name in ["=StTime=", "=StartDate="]:
                                    data["start_datetime"] = [log_data[row+1].strip()]
                                elif name in ["=EdTime=", "=EndDate="]:
                                    data["finish_datetime"] = [log_data[row+1].strip()]
                                elif name in ["=LaserPwr=", "=OrgPwrDp="]:
                                    data["laser_power"] = [log_data[row+1].strip()]
                                elif name in ["=Touka=", "=TransmitDp="]:
                                    data["transmittance"] = [log_data[row+1].strip()]
                                elif name in ["=MaxPwr=", "=PeakPwrDp="]:
                                    data["max_power"] = [log_data[row+1].strip()]
                                elif name in ["=Workpwr=", "=WorkPwrDp="]:
                                    data["work_power"] = [log_data[row+1].strip()]
                                elif name in ["=WkZ=", "=DataThkPerDp="]:
                                    data["laser_position"] = [log_data[row+1].strip()]
                                elif name in ["=SdePp="]:
                                    data["pp_patern"] = [log_data[row+1].strip()]
                                elif name in ["=SdeBe="]:
                                    data["be_pattern"] = [log_data[row+1].strip()]
                                elif name in ["=SdeBw="]:
                                    data["bw_pattern"] = [log_data[row+1].strip()]
                                elif name in ["=TactAll=", "=TotalTakt="]:
                                    data["total_time"] = [log_data[row+1].strip()]
                        data = pd.DataFrame(data)
                        data.insert(0, "id", file.split(".")[0])
                        data.insert(0, "machine", mc)
                        all_data = pd.concat([all_data, data], ignore_index=True)
        print(mc, len(all_data))

for col in all_data:
    if col in ["start_datetime", "finish_datetime"]:
        all_data[col] = pd.to_datetime(all_data[col], format="mixed")
    elif col not in ["machine", "id", "product", "dat_name", "pp_patern", "be_pattern", "bw_pattern"]:
        all_data[col] = pd.to_numeric(all_data[col])
    
all_data = all_data.sort_values(by=["start_datetime"], ignore_index=True)

engine = create_engine("mysql+pymysql://vip:123456@172.18.106.100/inadatabase")
with engine.connect() as con:
    for i in range(len(all_data)):
        try:
            all_data.loc[[i],:].to_sql("ad_log3", con, if_exists="append", index=False)
        except:
            pass