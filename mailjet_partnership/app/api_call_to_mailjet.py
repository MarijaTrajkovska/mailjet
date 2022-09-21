import time
import schedule
import pprint
from mailjet_rest import Client
import os
from app.mongo_api import MongoApi
from datetime import datetime, timedelta
from app.mailjet_accounts import mj_acc_keys


filters = {
    'ShowSubject': True,
    'ShowContactAlt': True,
    'Limit': 100,
    # 'FromTS': (datetime.now() - timedelta(1)).replace(microsecond=0).isoformat(),
    # 'ToTS': datetime.now().replace(microsecond=0).isoformat(),
    'FromTS': '2022-09-20T09:30:00',
    'ToTS': '2022-09-20T10:30:00',
    'Sort': "ArrivedAt+DESC"
}


def calling_mailjet_api():
    mongo_api = MongoApi(host="mongodb://dataScience:20PostoPopust.@65.21.194.47:27010/")

    for (mj_account, api_key_value, api_secret_value) in mj_acc_keys:
        api_key = os.environ['MJ_APIKEY_PUBLIC'] = api_key_value
        api_secret = os.environ['MJ_APIKEY_PRIVATE'] = api_secret_value

        mailjet = Client(auth=(api_key, api_secret), version='v3')

        offset = 0
        i = 1
        while True:
            print('TIME ', i)
            filters['Offset'] = offset
            result = mailjet.message.get(filters=filters)
            data = result.json()

            if data['Total'] == 0:
                break
            pprint.pprint(data)
            mongo_api.insert_data_to_mongo(data["Data"], mj_account)

            offset += 100
            i += 1


if __name__ == '__main__':

    # schedule.every().day.at('07:00').do(calling_mailjet_api)
    #
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)
     calling_mailjet_api()




