import pandas as pd
import os
import socket
from datetime import datetime as dt, timedelta as td
from sqlalchemy import create_engine, text
import numpy as np

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
    mc = item.Machine
    path = rf"\\{item.IP}\{item.Version}\_Log\Auto"
    if os.path.isdir(path) == False:
        path = rf"\\{item.IP}\{item.Version}\Auto"
        if os.path.isdir(path) == False:
            path = rf"\\{item.IP}\{item.Machine}\{item.Version}\Auto"
            
    if os.path.isdir(path) == True:
        for i in range(90,0,-1):
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
                                elif name in ["=LotN=,","=LotN=", "=LotNo=,","=LotNo="]:
                                    data["LotName"] = log_data[row+1].strip()
                                elif name in ["=StTime=", "=StartDate="]:
                                    data["start_datetime"] = log_data[row+1].strip()
                                elif name in ["=EdTime=", "=EndDate="]:
                                    data["finish_datetime"] = log_data[row+1].strip()
                                elif name in ["=LaserPwr=", "=OrgPwrDp="]:
                                    data["laser_power"] = log_data[row+1].strip()
                                elif name in ["=Touka=", "=TransmitDp="]:
                                    data["transmittance"] = log_data[row+1].strip()
                                elif name in ["=MaxPwr=", "=PeakPwrDp="]:
                                    data["max_power"] = log_data[row+1].strip()
                                elif name in ["=Workpwr=", "=WorkPwrDp="]:
                                    data["work_power"] = log_data[row+1].strip()
                                elif name in ["=WkZ=", "=DataThkPerDp="]:
                                    data["laser_position"] = log_data[row+1].strip()
                                elif name in ["=SdePp="]:
                                    data["pp_patern"] = [log_data[row+1].strip()]
                                elif name in ["=SdeBe="]:
                                    data["be_pattern"] = [log_data[row+1].strip()]
                                elif name in ["=SdeBw="]:
                                    data["bw_pattern"] = [log_data[row+1].strip()]
                                elif name in ["=TactAll=", "=TotalTakt="]:
                                    data["total_time"] = log_data[row+1].strip()
                                elif name in ["=WaferParameter=,", "=WaferParameter="]:
                                                df = log_data[row+2:]
                                                rows = []
                                                for line in log_data[row+2:]:
                                                    columns = [x.strip() for x in line.split(",")]
                                                    rows.append(columns)
                                                df = pd.DataFrame(rows)
                                  
                                                df_selected=df.iloc[:,3:4]
                                                df_selected.columns = ["wafer_thickness"]                                           
                                                df_selected=df_selected.copy()
                                                for col in df_selected.columns:

                                                 df_selected[col] = pd.to_numeric(df_selected[col], errors='coerce')
                                                 df_selected[col] = df_selected[col].replace([999,9999,99999], np.nan).replace(['999','9999','99999'], np.nan)
                                                 data[col] = str(round(df_selected[col].mean(skipna=True),2))
                                elif name in ["=CasSts=,","=CasSts=","=RecProps=","=RecProps=,"]:
                                                df=log_data[row+1:]
                                                rows=[]
                                                # statusx=0
                                                
                                                for line in df:
                                                    columns = [xs.strip() for xs in line.split(",")]
                                                    # if columns in ["121"] :
                                                    #     statusx=statusx+1
                                                        
                                                    rows.append(columns)
                                                df=pd.DataFrame(rows)
                                                    # df = df[~df.eq(['33','033 : OK']).any(axis=1)]
                                                df_selectedx=df.iloc[:,1:3]
                                                df_selecteds=df.iloc[:,1:3]
                                                df_selectedx.columns=["alarm","Status"]
                                                df_selecteds.columns=["alarm225","Status225"]
                                                df_121 = df_selectedx[df_selectedx["alarm"] == "121"]
                                                df_225 = df_selecteds[df_selecteds["alarm225"] == "225"]
                                                data["alarm"] = ', '.join(df_121["alarm"].astype(str))
                                                data["Status"] = ', '.join(df_121["Status"].astype(str))
                                                data["alarm225"] = ', '.join(df_225["alarm225"].astype(str))
                                                data["Status225"] = ', '.join(df_225["Status225"].astype(str))
                                                 






                        # for key in data.keys():
                        #     if isinstance(data[key],list):
                        #         data[key]=data[key][0] if len(data[key])>0 else None
                               
                                    
                               
                        data = pd.DataFrame([data])
                        data.insert(0, "id", file.split(".")[0])
                        data.insert(0, "machine", mc)
                        all_data = pd.concat([all_data, data], ignore_index=True)
        # print(all_data[["alarm","Status","LotName","wafer_thickness"]].head(10))
        print(mc, len(all_data))
        # print(all_data)
        # print(all_data.columns)
        # print(mc, len(all_data))
# for col in ["alarm","Status","LotName","wafer_thickness"]:
#     if col not in all_data.columns:
#         all_data[col]=""
# all_data=all_data.fillna("")
for col in all_data:
    if col in ["start_datetime", "finish_datetime"]:
        all_data[col] = pd.to_datetime(all_data[col], format="mixed",errors='coerce')
    elif col not in ["machine", "id", "product", "dat_name", "pp_patern", "be_pattern", "bw_pattern","alarm","Status","alarm225","Status225","LotName","wafer_thickness"]:
        all_data[col] = pd.to_numeric(all_data[col],errors='coerce')
    # for col in ["alarm","Status","LotName","wafer_thickness"]:
    #     if col in all_data.columns:
    #         all_data[col]=all_data[col].astype(str)
        
    
all_data = all_data.sort_values(by=["start_datetime"], ignore_index=True)

engine = create_engine("mysql+pymysql://vip:123456@172.18.106.100/inadatabase",echo=False)
with engine.connect() as con:
    for i in range(len(all_data)):
        try:
            row_df=all_data.iloc[[i]]
            row_df.to_sql("ad_log3", con, if_exists="append", index=False)
        except Exception as e:
            print(f"error {i}: {e}")
        
        # all_data.loc[[i],:].to_sql("ad_log", con, if_exists="append", index=False) 
       