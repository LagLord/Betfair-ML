# Getting the eventIDs i.e. each Greyhounds AU race  that is going to happen today which is stored in RACES

import requests
import json
import pandas as pd
import datetime

race_today = False

time_interval = datetime.timedelta(seconds=90)
RACES = []
BET_API_KEY = ""
SESSION_TOKEN = ""
API_KEY_AKKIO = ""

# Akkio API

akkio_make_dataset_url = "https://api.akk.io/v1/datasets"
akkio_make_dataset_params = {"name": "Greyhound Races data",
                             "api_key": API_KEY_AKKIO}

# Try to get the dataset for Greyhound races if it already exists or else create one

try:
    akkio_response = requests.get(akkio_make_dataset_url, params={"api_key": API_KEY_AKKIO})
    dataset_id = akkio_response.json()["datasets"][0]["id"]
except:
    akkio_response = requests.post(akkio_make_dataset_url, params=akkio_make_dataset_params)
    dataset_id = akkio_response.json()["dataset_id"]

# Betfair API headers and first call to get all the eventIDs Greyhound race in AU today

url = "https://api.betfair.com/exchange/betting/json-rpc/v1"
header = {'X-Application': BET_API_KEY, 'X-Authentication': SESSION_TOKEN, 'content-type': 'application/json'}

jsonrpc_req = '{"jsonrpc": "2.0", "method": "SportsAPING/v1.0/listEvents", "params": {"filter":{ ' \
              f'"eventTypeIds":["4339"], "marketCountries":["AU"], "marketStartTime": {"from": {datetime.datetime.utcnow().strftime("%Y-%m-%dT%TZ")},"to": {(datetime.datetime.utcnow() + datetime.timedelta(days=1)).strftime("%Y-%m-%dT%TZ")}}}}' \
              ', "id": 1} '

response1 = requests.post(url, data=jsonrpc_req, headers=header)
event_data = response1.json()

try:
    RACES = [item["event"]["id"] for item in event_data["result"]]
    print(f"{len(event_data['result'])} races today.")
    race_today = True
except:
    print("No Greyhound race in AU today!")
print(json.dumps(json.loads(response1.text), indent=3))

# Getting the marketID for each race in that will occur today using eventIDs

if race_today:
    for race in RACES:
        jsonrpc_req2 = '{"jsonrpc": "2.0", "method": "SportsAPING/v1.0/listMarketCatalogue", "params": {"filter": {' \
                       f'"eventIds": {race},"maxResults": "200","marketProjection": ["COMPETITION","EVENT",' \
                       '"EVENT_TYPE","MARKET_START_TIME"]},"id": 1} '
        response2 = requests.post(url, data=jsonrpc_req2, headers=header)
        data = response2.json()
        races_df = pd.DataFrame({
            "marketId": [race["marketId"] for race in data["result"]],
            "marketName": [race["marketName"] for race in data["result"]],
            "openDate": [race["event"]["openDate"] for race in data["result"]]
        })

        # Check race time for each row extracted from the API, is it after 90s or not and loop everytime

        while True:
            for index, row in races_df.iterrows():
                race_time = datetime.datetime.strptime(row["openDate"], "%Y-%m-%dT%TZ") - time_interval
                race_market_id = row["marketId"]
                race_name = row["marketName"]
                if race_time == datetime.datetime.utcnow():

                    # Get the bsp, pp_max, pp_wap and pp_volume for each runner

                    jsonrpc_req3 = '{"jsonrpc": "2.0","method": "SportsAPING/v1.0/listMarketBook","params": {' \
                                   f'"marketIds": [{race_market_id}],' \
                                   '"priceProjection": {"priceData": ["EX_BEST_OFFERS", "EX_TRADED", "SP_AVAILABLE", ' \
                                   '"SP_TRADED"],"virtualise": ' \
                                   '"true"}},"id": 1} '
                    response3 = requests.post(url, data=jsonrpc_req3, headers=header)
                    price_data = response3.json()

                    # Getting each price data in a list

                    for runner in price_data["result"]["runners"]:
                        bsp = runner["lastPriceTraded"]  # 1
                        pp_volume = runner["totalMatched"]  # 4
                        pp_max = runner["ex"]["tradedVolume"][-1]["price"]  # 2

                        # To calculate the average weight

                        weights = [float(item["price"]) * float(item["size"]) for item in runner["ex"]["tradedVolume"]]
                        sizes = [float(item["size"]) for item in runner["ex"]["tradedVolume"]]
                        pp_wap = round((sum(weights) / sum(sizes)), 2)  # 3
                        market_id = race_market_id
                        date = race_time + time_interval

                        # Enter data into Akkio dataset

                        akkio_add_row_params = {
                            "rows": [
                                {"event_date": date, "market_id": market_id, "market_name": race_name, "bsp": bsp,
                                 "pp_max": pp_max, "pp_wap": pp_wap, "pp_volume": pp_wap}],
                            "id": dataset_id,
                            "api_key": API_KEY_AKKIO
                        }
                        response_akkio_post = requests.post(url=akkio_make_dataset_url, params=akkio_add_row_params)
                        print(response_akkio_post.json()["status"])

