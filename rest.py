from bclib import edge

def mongo_connection():
    from pymongo import MongoClient
    client = MongoClient("mongodb://localhost:27017")
    db = client["Manzoomeh"]
    return db

options = {
    "server": "localhost:8080",
    "router": "restful",
    "log_request": False
}

app = edge.from_options(options)

@app.cache()
async def get_db():
    db = mongo_connection()
    return db

@app.restful_action(
    app.url(":own_id/:dmn_id"))
async def process_restful_request(context: edge.RESTfulContext):
    print("process_restful_request")
    own_id = int(context.url_segments.own_id)
    dmn_id = int(context.url_segments.dmn_id)
    db = get_db()
    print(f"Fetch Data from {own_id}-{dmn_id} Table")
    colls = db[f"{own_id}-{dmn_id}"].find({},{"_id":0})
    collsInList = []
    for coll in colls:
        collsInList.append(coll)
    return collsInList


@app.restful_action(
    app.url(":own_id/:dmn_id/urls/:url"))
#WONT WORK! URL contains /
async def process_filtered_by_url_restful_request(context: edge.RESTfulContext):
    print("process_filtered_restful_request")
    own_id = int(context.url_segments.own_id)
    dmn_id = int(context.url_segments.dmn_id)
    url = str(context.url_segments.url)
    db = get_db()
    print(f"{own_id}-{dmn_id}")
    colls = db[f"{own_id}-{dmn_id}"].find({"url":url},{"_id":0, "owner_id":0, "domain_id":0})
    collsInList = []
    for coll in colls:
        collsInList.append(coll)
    return collsInList


@app.restful_action(
    app.url(":own_id/:dmn_id/date/:date"))
async def process_filtered_by_date_restful_request(context: edge.RESTfulContext):
    print("process_filtered_by_DATE_restful_request")
    own_id = int(context.url_segments.own_id)
    dmn_id = int(context.url_segments.dmn_id)
    date = str(context.url_segments.date)
    db = get_db()
    print(f"{own_id}-{dmn_id}")
    colls = db[f"{own_id}-{dmn_id}"].find({"date":date},{"_id":0 ,"owner_id":0, "domain_id":0})
    collsInList = []
    for coll in colls:
        collsInList.append(coll)
    return collsInList

@app.restful_action(
    app.url(":own_id/:dmn_id/maxhit"))
async def process_restful_request_max_hit(context: edge.RESTfulContext):

    print("process_filtered_by_DATE_restful_request")
    own_id = int(context.url_segments.own_id)
    dmn_id = int(context.url_segments.dmn_id)
    db = get_db()
    coll = db[f"{own_id}-{dmn_id}"]
    sorted = coll.find({},{"_id":0, "url":1, "hit":1}).sort("hit",-1) #.limit(1)
    coll_list = []
    for c in sorted:
        coll_list.append(c)
    return coll_list

@app.restful_action(
    app.url(":own_id/:dmn_id/date/:date/hours"))
async def get_hours(context: edge.RESTfulContext):
    print("sort_hours_by_date")
    own_id = int(context.url_segments.own_id)
    dmn_id = int(context.url_segments.dmn_id)
    date = str(context.url_segments.date)
    db = get_db()
    coll = db[f"{own_id}-{dmn_id}"]
    coll = coll.find({"date":date},{"time":1, "url":1,"_id":0}).sort("time.hit",-1)
    coll_list = []
    for c in coll:
        coll_list.append(c)
    return coll_list
    
@app.restful_action(
    app.url(":own_id/:dmn_id/{:date}"))

async def get_hours(context: edge.RESTfulContext):
    print("sort_hours_by_date")
    own_id = int(context.query.file_from_list)
    dmn_id = int(context.url_segments.dmn_id)
    city = str(context.url_segments.city)
    db = get_db()
    coll = db[f"{own_id}-{dmn_id}"]
    coll = coll.find({},{"hit":1, "url":1,"_id":0,"uai_info.device_id":1})
    coll_list = []
    for c in coll:
        coll_list.append(c)
    return coll_list

@app.restful_action(
    app.url(":own_id/:dmn_id/date/:date/device_id/:deviceid"))

async def get_date_device_id(context: edge.RESTfulContext):
    print("get_date_device_id")
    own_id = int(context.query.file_from_list)
    dmn_id = int(context.url_segments.dmn_id)
    date = str(context.url_segments.date)
    d_id = int(context.url_segments.deviceid)
    db = get_db()
    coll = db[f"{own_id}-{dmn_id}"]
    found = coll.find({"uai_info.device_ids": {"device_id" :0}} ,{"hit":1, "url":1,"_id":0,"uai_info":1})
    #coll = coll.find({"uai_info":{"device_ids":{"device_id":d_id}}, "date":date},{"hit":1, "url":1,"_id":0,"uai_info":1})
    print(found)
    coll_list = []
    for c in found:
        coll_list.append(c)
        print(c)
    return coll_list


app.listening()