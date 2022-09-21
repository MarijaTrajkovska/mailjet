from pymongo import MongoClient
from app.client_finder import find_client_from_subject

valid_eventes = ['sent', 'opened', 'clicked', 'spam', 'hardbounced', 'softbounced', 'unsub']


class MongoApi:

    def __init__(self,
                 host="",
                 database="scrapers",
                 coll_mailjet="mj_test",
                 coll_partnership="partnership_test"
                 ):
        self.host = host
        self.database = database
        self.coll_mailjet = coll_mailjet
        self.coll_partnership = coll_partnership

    def change_stats_in_partnership_database(self, event, email, client_name):
        with MongoClient(host=self.host) as client:
            pship_coll = client[self.database][self.coll_partnership]
            stats_tmp = list(pship_coll.aggregate([
                {
                    '$unwind': '$' + "clients"
                },
                {
                    '$match': {
                        'email': email,
                        'clients.client': client_name
                    }
                },
                {
                    '$project': {
                        'client': '$clients.client',
                        'sent': '$clients.sent',
                        'opened': '$clients.opened',
                        'clicked': '$clients.clicked',
                        'spam': '$clients.spam',
                        'unsub': '$clients.unsub',
                        'hardbounced': '$clients.hardbounced',
                        'softbounced': '$clients.softbounced'
                    }
                }
            ]))

            event_num = stats_tmp[0][event]
            pship_coll.update_one({
                "email": email,
                "clients": {"$elemMatch": {"client": client_name}}},
                {"$set": {"clients.$." + event: event_num + 1}}
            )

    def update_activity_score(self, email, event):
        with MongoClient(host=self.host) as client:
            pship_coll = client[self.database][self.coll_partnership]
            score = pship_coll.find_one({
                "email": email
            })['activity_score']

            pship_coll.update_one({
                "email": email},
                {"$set": {"activity_score": score + calculate_new_activity_score(event)}}
            )

    def change_stats_in_mailjet_database(self, event, mj_email, client_name):
        with MongoClient(host=self.host) as client:
            mj_coll = client[self.database][self.coll_mailjet]
            stats_tmp = list(mj_coll.aggregate([
                {
                    '$unwind': '$mailjet_acc'
                },
                {
                    '$match': {
                        'client': client_name,
                        'mailjet_acc.email': mj_email
                    }
                },
                {
                    '$project': {
                        'email': '$mailjet_acc.email',
                        'sent': '$mailjet_acc.sent',
                        'opened': '$mailjet_acc.opened',
                        'clicked': '$mailjet_acc.clicked',
                        'spam': '$mailjet_acc.spam',
                        'unsub': '$mailjet_acc.unsub',
                        'hardbounced': '$mailjet_acc.hardbounced',
                        'softbounced': '$mailjet_acc.softbounced'
                    }
                }
            ]))

            event_num = stats_tmp[0][event]
            mj_coll.update_one({
                "client": client_name,
                "mailjet_acc": {"$elemMatch": {"email": mj_email}}},
                {"$set": {"mailjet_acc.$." + event: event_num + 1}}
            )

    def add_new_client_for_email(self, email, event, client_name):
        new_client = create_new_item('client', client_name, event)
        with MongoClient(host=self.host) as client:
            pship_coll = client[self.database][self.coll_partnership]
            pship_coll.update_one({'email': email}, {'$push': {'clients': new_client}})

    def add_new_mjemail_for_client(self, email, event, client_name):
        new_email = create_new_item('email', email, event)
        with MongoClient(host=self.host) as client:
            mj_coll = client[self.database][self.coll_mailjet]
            mj_coll.update_one({'client': client_name}, {'$push': {'mailjet_acc': new_email}})

    def create_new_email_in_database(self, email, event, client_name):
        new_client = create_new_item('client', client_name, event)
        activity_score = calculate_new_activity_score(event)
        new_email = {
            'email': email,
            'activity_score': activity_score,
            'clients': [new_client]
        }
        with MongoClient(host=self.host) as client:
            pship_coll = client[self.database][self.coll_partnership]
            pship_coll.insert_one(new_email)

    def create_new_client_in_database(self, mj_email, event, client_name):
        new_mj_email = create_new_item('email', mj_email, event)
        new_client = {
            'client': client_name,
            'mailjet_acc': [new_mj_email]
        }
        with MongoClient(host=self.host) as client:
            mj_coll = client[self.database][self.coll_mailjet]
            mj_coll.insert_one(new_client)

    def insert_data_to_mongo(self, list_dict, mailjet_acc_email):
        for dict in list_dict:
            event = dict["Status"]
            if event not in valid_eventes:
                continue

            email = dict["ContactAlt"]
            client_name = find_client_from_subject(dict["Subject"])
            mj_email = mailjet_acc_email
            with MongoClient(host=self.host) as client:
                mj_coll = client[self.database][self.coll_mailjet]
                pship_coll = client[self.database][self.coll_partnership]

                is_email_in_database = pship_coll.count_documents({"email": email})
                if is_email_in_database != 0:

                    self.update_activity_score(email, event)

                    is_client_in_database = pship_coll.find_one({
                        "email": email,
                        "clients": {"$elemMatch": {"client": client_name}}}
                    )

                    if is_client_in_database is not None:
                        self.change_stats_in_partnership_database(event, email, client_name)
                        print("PARTNERSHIP DATABASE: Changing stats in existing client for email")

                    else:
                        self.add_new_client_for_email(email, event, client_name)
                        print("PARTNERSHIP DATABASE: Adding new client for email")

                else:
                    self.create_new_email_in_database(email, event, client_name)
                    print("PARTNERSHIP DATABASE: Creating new email in database")

                is_client_in_database = mj_coll.count_documents({"client": client_name})
                if is_client_in_database != 0:

                    is_mj_email_in_database = mj_coll.find_one({
                        "client": client_name,
                        "mailjet_acc": {"$elemMatch": {"email": mj_email}}}
                    )

                    if is_mj_email_in_database is not None:
                        self.change_stats_in_mailjet_database(event, mj_email, client_name)
                        print("MAILJET DATABASE: Changing stats for existing mailjet acc")

                    else:
                        self.add_new_mjemail_for_client(mj_email, event, client_name)
                        print("MAILJET DATABASE: Adding new mailjet in existing client")

                else:
                    self.create_new_client_in_database(mj_email, event, client_name)
                    print("MAILJET DATABASE: Adding new client")



def create_new_item(item_type, item, event):
    item = {item_type: item,
            'sent': 0,
            'opened': 0,
            'clicked': 0,
            'spam': 0,
            'unsub': 0,
            'hardbounced': 0,
            'softbounced': 0
            }
    item[event] += 1
    return item


def calculate_new_activity_score(event):
    if event == 'sent':
        return 0
    elif event == 'opened':
        return 0.3
    elif event == 'clicked':
        return 0.9
    elif event in ['spam', 'unsub']:
        return -50
    elif event == 'hardbounced':
        return -100
    else:
        return -1


