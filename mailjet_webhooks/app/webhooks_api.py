import pprint
from fastapi import FastAPI, Request
from mailjet_webhooks.app.counter import counter_func
import asyncio
import uvicorn

# uvicorn new_api_bobby:app --host 0.0.0.0 --port 9988 --reload
# http://185.234.128.212:9988/callback

app = FastAPI(title='Partnership')


@app.get("/")
async def root():
    return {"message": "Partnership"}


@app.post("/callback")
async def callback_mailjet(item: Request):
    # http://185.234.128.212:9988/callback
    # data1 = pd.read_csv('data.csv')
    # count_data = pd.read_csv('count_data.csv')
    # db = pd.read_csv('counter1.csv')
    data = await item.json()
    pprint.pprint(data)
    # counter_func(data, db, 'bobby@foodbygracja.com')
    # fields2(data, db, 'bobby@foodbygracja.com')
    # fields(data, data1, count_data)
    await asyncio.sleep(0.1)
    return data


@app.post("/callback_charlotte")
async def callback_from_charlotte(item: Request):
    # db = pd.read_csv('counter1.csv')
    data = await item.json()
    pprint.pprint(data)
#    counter_func(data, 'charlotte@creatorspack.com')
    await asyncio.sleep(0.1)
    return data


@app.post("/callback_emily")
async def callback_from_emily(item: Request):
    # http://185.234.128.212:9988/callback_emily
    # db = pd.read_csv('counter1.csv')
    data = await item.json()
    mailjet_acc = 'emily@getwilla.com'
    pprint.pprint(data)
   # counter_func(data, mailjet_acc)
    await asyncio.sleep(0.1)
    return data


@app.post("/callback_doris")
async def callback_from_doris(item: Request):
    # http://185.234.128.212:9988/callback_doris
    # db = pd.read_csv('counter1.csv')
    data = await item.json()
    mailjet_acc = 'doris@creatormanagements.com'
    pprint.pprint(data)
    counter_func(data,'doris@creatormanagements.com')
    await asyncio.sleep(0.1)
    return data


# @app.post("/callback_ash")     SUSPENDIRAN E
# async def callback_from_ash(item: Request):
#     # db = pd.read_csv('counter1.csv')
#     data = await item.json()
#     mailjet_acc = 'ash@sosocialadvertising.com'
#     pprint.pprint(data)
#     counter_func(data, 'ash@sosocialadvertising.com')
#     await asyncio.sleep(0.1)
#     return data


@app.post("/callback_ana")
async def callback_ash(item: Request):
    # http://185.234.128.212:9988/callback_ana
    # db = pd.read_csv('counter1.csv')
    data = await item.json()
    mailjet_acc = 'anavukas6@gmail.com'
    pprint.pprint(data)
    counter_func(data, 'anavukas6@gmail.com')
    await asyncio.sleep(0.1)
    return data


if __name__ == '__main__':
    uvicorn.run("webhooks_api:app", port=9988, host='0.0.0.0', reload=True)
