import pymongo
client = pymongo.MongoClient("mongodb://dataScience:20PostoPopust.@65.21.194.47:27010/")
db = client.scrapers
mj_collection = db['mj_test']


def create_new_doc(email, event):
    item = {'email': email,
            'sent': 0,
            'time_sent': [],
            'open': 0,
            'time_open': [],
            'click': 0,
            'time_click': [],
            'hard_bounce': 0,
            'soft_bounce': 0,
            'spam': 0,
            'unsub': 0,
            'ip': [],
            'geo': [],
            'agent': []}

    item[event] += 1
    return item

def update_event_stats(doc, event):
    tmp = doc[event]
    doc[event] = tmp + 1
    return doc


# sent, open, click, unsub, spam,
def counter_func(data, db, mailjet_acc):
    if type(data) == list:
        data = data[0]
        email = data.get('email')
        event = data['event']
        db_doc = mj_collection.find_one({"email": email})
        if db_doc is None:
            print('New event -> ' + event)
            if event == 'bounce':
                if data['hard_bounce'] is True:
                    new_doc = create_new_doc(email, 'hard_bounce')
                else:
                    new_doc = create_new_doc(email, 'soft_bounce')
            else:
                new_doc = create_new_doc(email, event)
                if event == 'sent' or event == 'open' or event == 'click':
                    new_doc['time_' + event].append(data['time'])
                if event == 'click':
                    new_doc['ip'].append(data['ip'])
                    new_doc['geo'].append(data['geo'])
                    new_doc['agent'].append(data['agent'])

            mj_collection.insert_one(new_doc)
        else: ######## na ovie nadole nekako treba update da se naprave
            if event == 'bounce':
                if data['hard_bounce'] is True:
                    db_doc = update_event_stats(db_doc, 'hard_bounce')
                else:
                    db_doc = update_event_stats(db_doc, 'soft_bounce')
            else:
                db_doc = update_event_stats(db_doc, event)
                if event == 'sent' or event == 'open' or event == 'click':
                    db_doc['time_' + event].append(data['time'])
                if event == 'click':
                    db_doc['ip'].append(data['ip'])
                    db_doc['geo'].append(data['geo'])
                    db_doc['agent'].append(data['agent'])













        # else:
        #     if data['event'] == 'open':
        #         print('OPEN')
        #         db.loc[:, 'open'][db['email'] == data['email']] += 1
        #         db.loc[:, 'time_open'][db['email'] == data['email']] = data['time']
        #         db.to_csv('counter1.csv', index=False)
        #     elif data['event'] == 'click':
        #         print('CLICK')
        #         db.loc[:, 'click'][db['email'] == data['email']] += 1
        #         db.loc[:, 'time_click'][db['email'] == data['email']] = data['time']
        #         db.loc[:, 'ip'][db['email'] == data['email']] = data['ip']
        #         db.loc[:, 'geo'][db['email'] == data['email']] = data['geo']
        #         db.to_csv('counter1.csv', index=False)
        #     elif data['event'] == 'bounce':
        #         print('BOUNCE')
        #         hb = data['hard_bounce']
        #         if hb != True:
        #             db.loc[:, 'soft_bounce'][db['email'] == data['email']] += 1
        #         else:
        #             db.loc[:, 'hard_bounce'][db['email'] == data['email']] += 1
        #         db.to_csv('counter1.csv', index=False)
        #     elif data['event'] == 'spam':
        #         print('SPAM')
        #         db.loc[:, 'spam'][db['email'] == data['email']] += 1
        #         db.to_csv('counter1.csv', index=False)
        #     elif data['event'] == 'blocked':
        #         print('BLOCKED')
        #     elif data['event'] == 'unsub':
        #         print('UNSUB')
        #         db.loc[:, 'unsub'][db['email'] == data['email']] += 1
        #         db.to_csv('counter1.csv', index=False)