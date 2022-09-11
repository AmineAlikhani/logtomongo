from concurrent.futures import process
import json

def sql_connection():
    import pyodbc
    server = '172.20.20.200,1433'
    database = 'domains'
    username = 'sa'
    password = 'Salam1Salam2'
    connection = 'DRIVER={SQL Server};SERVER=' + server + \
        ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password
    conn = pyodbc.connect(connection)
    curs = conn.cursor()
    return curs  


def absoute_url(url:str):
    import re
    url= re.split("/", url,1)
    absoute_url = url[1]
    return absoute_url


def split_date_hour(date:str):
    import re
    date = re.split(" ",date)
    time = date[1]
    date = date[0]
    time = re.split(":",time)
    hour = time[0]
    return date, hour


def ip_info(ip:int):
    import json
    f = open("Files/IpData/ip_int_json.txt", "r")
    ip_infos = json.loads(f.read())
    for row in ip_infos:
        if ip in range(row['first_ip'], row['last_ip']+1):
            ip_info = {
                "city" : row['city'],
                "provider" :  row['company']
            }
            break
        ip_info = {
            "city": "",
            "provider" : ""  
        }
    if ip_info:
        pass
    else:
        print("Ip info was not found!")
    f.close()
    return ip_info


def fetch_data(own_id: int, dmn_id: int):
    import glob
    logs = glob.glob(f"Files/{own_id}/Domain Log/{dmn_id}/**.log")
    
    print("Reading raw data")
    db=[]

    for log in logs:
        h = open(log, "r")
        for line in h:
            try:
                data = json.loads(line)
            except:
                data = line.strip()
                data = json.loads(line[3:])

            date, hour = split_date_hour(data["data"]["request_time"])
            data = {
                "owner_id": data["owner_id"],
                "domain_id": data["domain_id"],
                "url": absoute_url(data["data"]["url"]),
                "user_agent_id": data["data"]["user_agent_id"],
                "status_code": data["data"]["status_code"],
                "ip": ip_info(data["data"]["ip"]),
                "hour": hour,
                "date": date,
                }
            db.append(data)
        h.close()
    print("Ready to Struct!")
    return(db)

    
def move_logs(adress,own_id, dmn_id):
    import os
    import shutil
    own_id  = str(own_id)
    dmn_id = str(dmn_id)
    files = os.listdir(adress)
    dest = "ProcessedFiles"
    if os.path.exists(dest):
        print("here1")
        pass
    else:
        os.mkdir("ProcessedFiles")
    dest = os.path.join(dest, own_id)
    if os.path.exists(dest):
        print("here2")
        pass
    else:
        os.mkdir(dest)
    dest = os.path.join(dest, dmn_id)
    if os.path.exists(dest):
        print("here3")
        pass
    else:
        os.mkdir(dest)
    for log in files:        
        print("0000000000",log)
        shutil.move(os.path.join(adress,log), os.path.join(dest,log))

        

def deviceID(user_agent):
    arr =(user_agent.is_mobile and 2)or(user_agent.is_tablet and 4)or(user_agent.is_pc and 1)or(0)
    return (arr)


def uai(db):
    from user_agents import parse
    import pyodbc
    ua_ids=[]
    uai_db=[]
    print("Checking for New User Agent Ids..")
# Create ua_ids    
    for d in db:
        if d["user_agent_id"] not in ua_ids:
            ua_ids.append(d["user_agent_id"]) 
    curs = sql_connection()
# ssms connection, fetch data to uai_db
    for id in ua_ids:
        sql =  "SELECT useragent , isbot, ismobile, deviceid from agents WHERE id = ? "
        curs.execute(sql,id)
        row = str(curs.fetchone())
        if row:
            row = parse(row)
            d = {
                "user_agent_id": id,

                "device_id" : deviceID(row),
# developing needed
                "browser_name": row.browser.family,
                "browser_version": row.browser.version_string,

                "os_name": row.os.family,
                "os_version": row.os.version_string,

                "is_bot": row.is_bot,                
            }
            uai_db.append(d)
            print("Found it!")
# Save uai_db 
    server = '172.20.20.200,1433'
    database = 'domains'
    username = 'sa'
    password = 'Salam1Salam2'
    connection = 'DRIVER={SQL Server};SERVER=' + server + \
        ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password
    conn = pyodbc.connect(connection)   
    curs = conn.cursor()

    try:
        curs.execute('''
        CREATE TABLE uai(
        user_agent_id int,
        device_id int,
        os_name nvarchar(50),
        os_version nvarchar(20),
        browser_name nvarchar(50),
        browser_version nvarchar(20),
        is_bot int
        )''')
        conn.commit()
    except:
        pass 
    for d in uai_db:
        curs.execute('''
        IF NOT EXISTS (SELECT * FROM uai WHERE user_agent_id = ?)
        BEGIN
            INSERT INTO uai(
                user_agent_id,
                device_id,
                os_name, 
                os_version,
                browser_name,
                browser_version,
                is_bot
                ) 
            VALUES (?,?,?,?,?,?,?)
        END
                ''',              
                (
                d["user_agent_id"],
                d["user_agent_id"],
                d["device_id"],
                d["os_name"],
                d["os_version"],
                d["browser_name"],
                d["browser_version"],
                d["is_bot"]))
        conn.commit()
    conn.close()

    for data in db:
        for d in uai_db:
            if d["user_agent_id"] == data["user_agent_id"]:  
                data["uai_info"] = {
                    "device_id": d["device_id"],
                    "os_name": d["os_name"],
                    "os_version": d["os_version"],
                    "browser_name": d["browser_name"],
                    "browser_version": d["browser_version"],
                    "is_bot": d["is_bot"]    
                    }
    print("User Agents Ids Checked!")
    return db

def create_url_based_db(db:list):

    url_date = []
    for d in db:
        if f"{d['url']}-{d['date']}" not in url_date:
            print(f"{d['url']}-{d['date']}")
            url_date.append(f"{d['url']}-{d['date']}")
            sd = struct_data(db, d["url"], d["date"])
            mongo(sd, dmn_id=d["domain_id"], own_id=d["owner_id"], url=d["url"], date=d["date"])


def struct_data(db:list, url:str, date):

    import pandas as pd
    url_date_based_db = []
    
    for d in db:
        if d["url"] == url and d["date"] == date:
            url_date_based_db.append(d)

    for d in url_date_based_db:
        owner_id = d["owner_id"]
        domain_id = d["domain_id"]
        break

    print(f"Found data for {d['url']} at {d['date']}")
    df = pd.json_normalize(url_date_based_db)
    print(df)
    print(f"Fixing Data Structure...")

    # Find unique
    h_list = []
    city_list = []
    provider_list = []
    dId_list = []
    os_list = []
    os_v_list = []
    bro_list = []
    bro_v_list = []
    bot_list = []
    for d in url_date_based_db:
            if d["hour"] not in h_list:
                h_list.append(d["hour"])
            if d["ip"]["city"] not in city_list:
                city_list.append(d["ip"]["city"])
            if d["ip"]["provider"] not in provider_list:
                provider_list.append(d["ip"]["provider"])
            if d["uai_info"]["device_id"] not in dId_list:
                dId_list.append(d["uai_info"]["device_id"])
            if d["uai_info"]["os_name"] not in os_list:
                os_list.append(d["uai_info"]["os_name"])
            if d["uai_info"]["os_version"] not in os_v_list:
                os_v_list.append(d["uai_info"]["os_version"])
            if d["uai_info"]["browser_name"] not in bro_list:
                bro_list.append(d["uai_info"]["browser_name"])
            if d["uai_info"]["browser_version"] not in bro_v_list:
                bro_v_list.append(d["uai_info"]["browser_version"])
            if d["uai_info"]["is_bot"] not in bot_list:
                bot_list.append(d["uai_info"]["is_bot"])
        
    # Count 
    hours = []  
    cities = [] 
    providers = []
    d_ids = []
    oss = []
    osvs = []
    bs = []
    bvs = []
    bots =[]

    hit = int(df["url"].value_counts()[url])

    for id in dId_list:
        d = {
            "device_id": id,
            "hit": int(df["uai_info.device_id"].value_counts(id)),
            "pct": int(df["uai_info.device_id"].value_counts(id))/hit,
        }
        d_ids.append(d)

    for o in os_list:
        d = { "os" : o,
            "hit" :int(df["uai_info.os_name"].value_counts()[o]),
            "pct" : int(df["uai_info.os_name"].value_counts()[o])/hit,
        }
        oss.append(d)

    for ov in os_v_list:
        d = { "os_version" : ov,
            "hit" : int(df["uai_info.os_version"].value_counts()[ov]),
            "pct" : int(df["uai_info.os_version"].value_counts()[ov])/hit,
        }
        osvs.append(d)

    for b in bro_list:
        d = { "browser" : b,
            "hit" : int(df["uai_info.browser_name"].value_counts()[b])
        }
        bs.append(d)

    for bv in bro_v_list:
        d = { "browser_version" : bv,
            "hit" : int(df["uai_info.browser_version"].value_counts()[bv])
        }
        bvs.append(d)

    for b in bot_list:
        if b == True:
            d = { "bot" : True,
            "hit" : int(df["uai_info.is_bot"].value_counts()[True])
            }
            bots.append(d)
            break

    for h in h_list:
        d = { "hour" : h,
            "hit" : int(df["hour"].value_counts()[h])
        }
        hours.append(d)
    
    for c in city_list:
        d = { "city" : c,
            "hit" : int(df["ip.city"].value_counts()[c])
        }
        cities.append(d)

    for p in provider_list:
        d = { "provider" :p,
            "hit" : int(df["ip.provider"].value_counts()[p])
        }
        providers.append(d)

    sd = {
        "owner_id": owner_id,
        "domain_id": domain_id,
        "url": url,
        "date": date,
        "hit": int(df["url"].value_counts()[url]),
        "time": hours,   
        "ip_info":{
            "cities" : cities,
            "providers": providers
        },
        "uai_info":{
            "device_ids" : d_ids,
            "os" : oss,
            "os_version": osvs,
            "browser": bs,
            "browser_version": bvs,
            "bots": bots
        } 
    }
    print("Fixed!")
    return sd

def mongo(sd:list, own_id:int, dmn_id:int, url:str, date:str):
    print("Connecting to mongodb")
    from pymongo import MongoClient
#    client = MongoClient("mongodb://172.20.20.220:27017")
    client = MongoClient("mongodb://localhost:27017")
    db = client["Manzoomeh"]
    coll = db[f"{own_id}-{dmn_id}"]
    q = {"url" : url, "date":date }
    new ={"$set" : sd}
    coll.update_one(q, new, upsert=True)
    print("Data inserted on Mongodb")
    

def get_all():
    import glob
    adress = f'Files/**/Domain Log/**'
    for log_file in glob.glob(adress):
        print("aaaaaaa",log_file)
        a= log_file.split("\\")
        owner_id = int(a[1])
        domain_id = int(a[3])
        print(f"Fetching log for: {owner_id},{domain_id}")
        db = fetch_data(owner_id, domain_id)
        move_logs(log_file, owner_id, domain_id)
        db_fixed = uai(db)
        create_url_based_db(db_fixed)
'''
def get_daily():
    import glob
    from datetime import datetime
    date = datetime.now().strftime("%Y_%m_%d")
    date = "2022-07-25"
    adress = f'Files/**/Domain Log/**/{date}.log'
    print(adress)
    for log_file in glob.glob(adress):
        print(log_file)
        a= log_file.split("\\")
        owner_id = int(a[1])
        domain_id = int(a[3])
        print(f"Fetching log for: {owner_id},{domain_id}")
        db = fetch_data(owner_id, domain_id)
        db_fixed = uai(db)
        update_url_based_db(db_fixed)
'''

get_all()