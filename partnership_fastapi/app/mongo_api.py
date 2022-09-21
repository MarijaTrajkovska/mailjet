import pandas as pd
from pymongo import MongoClient


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

    def get_list_top_emails_from_database(self):
        with MongoClient(host=self.host) as client:
            pship_coll = client[self.database][self.coll_partnership]
            tmp_list = list(pship_coll.find({"$and": [{"activity_score": {'$gte': 1}},
                                                      {"clients.unsub": {"$eq": 0}},
                                                      {"clients.spam": {"$eq": 0}},
                                                      {"clients.hardbounced": {"$eq": 0}},
                                                      ]},
                                            {"_id": 0, "email": 1}))
        emails = []
        for el in tmp_list:
            emails.append(el['email'])
        return emails

    def get_list_worst_emails_from_database(self):
        with MongoClient(host=self.host) as client:
            pship_coll = client[self.database][self.coll_partnership]
            tmp_list = list(pship_coll.find({"activity_score": {'$lte': -1}},
                                            {"_id": 0, "email": 1}))

        emails = []
        for el in tmp_list:
            emails.append(el['email'])
        return emails

    def find_good_emails_from_df(self, lookup_emails):
        good_emails = self.get_list_top_emails_from_database()
        match = set(good_emails).intersection(lookup_emails)

        return match

    def find_bad_emails_from_df(self, lookup_emails):
        bad_emails = self.get_list_worst_emails_from_database()
        match = set(bad_emails).intersection(lookup_emails)

        return match

    def filter_emails(self, file_path, w):
        df = pd.read_csv(file_path)
        lookup_emails = list(df.email.unique())
        good_emails = self.find_good_emails_from_df(lookup_emails)
        good = df[df['email'].isin(good_emails)]
        bad_emails = self.find_bad_emails_from_df(lookup_emails)
        bad = df[df['email'].isin(bad_emails)]

        if w == "good":
            return good
        elif w == "bad":
            return bad
        else:
            return good, bad

    def get_list_worst_emails_for_client(self, client_name):
        with MongoClient(host=self.host) as client:
            pship_coll = client[self.database][self.coll_partnership]
            tmp_list = list(pship_coll.find({
                "clients": {
                    "$elemMatch": {
                        "client": client_name,
                        "$or": [
                            {"unsub": {"$gt": 0}},
                            {"spam": {"$gt": 0}},
                            {"hardbounced": {"$gt": 0}}
                        ]
                    },
                }
            },
                {"_id": 0, "email": 1}
            ))

        emails = []
        for el in tmp_list:
            emails.append(el['email'])
        return emails

    def get_list_top_emails_for_client(self, client_name):
        with MongoClient(host=self.host) as client:
            pship_coll = client[self.database][self.coll_partnership]
            tmp_list = list(pship_coll.find({
                "clients": {
                    "$elemMatch": {
                        "client": client_name,
                        "$and": [
                            {"$or": [
                                {"clicked": {'$gte': 1}},
                                {"opened": {'$gte': 1}}
                            ]},
                            {"unsub": {"$eq": 0}},
                            {"spam": {"$eq": 0}},
                            {"hardbounced": {"$eq": 0}}
                        ]
                    },
                }
            },
                {"_id": 0, "email": 1}
            ))

        emails = []
        for el in tmp_list:
            emails.append(el['email'])
        return emails

    def find_good_emails_from_df_with_client(self, lookup_emails, client):
        good_emails = self.get_list_top_emails_for_client(client)
        match = set(good_emails).intersection(lookup_emails)

        return match

    def find_bad_emails_from_df_with_client(self, lookup_emails, client):
        bad_emails = self.get_list_worst_emails_for_client(client)
        match = set(bad_emails).intersection(lookup_emails)

        return match

    def filter_emails_with_client(self, file_path, client, w):
        df = pd.read_csv(file_path)
        lookup_emails = list(df.email.unique())
        good_emails = self.find_good_emails_from_df_with_client(lookup_emails, client)
        good = df[df['email'].isin(good_emails)]
        bad_emails = self.find_bad_emails_from_df_with_client(lookup_emails, client)
        bad = df[df['email'].isin(bad_emails)]

        if w == "good":
            return good
        elif w == "bad":
            return bad
        else:
            return good, bad

    def get_bad_emails_statistics_for_client(self, client_name):
        with MongoClient(host=self.host) as client:
            pship_coll = client[self.database][self.coll_partnership]
            tmp_list = list(pship_coll.find({
                "clients": {
                    "$elemMatch": {
                        "client": client_name,
                        "$or": [
                            {"unsub": {"$gt": 0}},
                            {"spam": {"$gt": 0}},
                            {"hardbounced": {"$gt": 0}}
                        ]
                    },
                }
            },
                {'_id': 0, 'email': 1, "clients.$": 1}
            ))
        df = df_from_list_of_dicts(tmp_list)
        df.sort_values(by=['spam', 'unsub', 'hardbounced'], ascending=False, inplace=True)
        return df

    def get_good_emails_statistics_for_client(self, client_name):
        with MongoClient(host=self.host) as client:
            pship_coll = client[self.database][self.coll_partnership]
            tmp_list = list(pship_coll.find({
                "clients": {
                    "$elemMatch": {
                        "client": client_name,
                        "$and": [
                            {"$or": [
                                {"clicked": {'$gte': 1}},
                                {"opened": {'$gte': 1}}
                            ]},
                            {"unsub": {"$eq": 0}},
                            {"spam": {"$eq": 0}},
                            {"hardbounced": {"$eq": 0}}
                        ]
                    },
                }
            },
                {'_id': 0, 'email': 1, "clients.$": 1}
            ))
        df = df_from_list_of_dicts(tmp_list)
        df.sort_values(by=['opened', 'clicked', 'sent'], ascending=False, inplace=True)
        return df


def df_from_list_of_dicts(tmp_list):
    data = []
    for t in tmp_list:
        data.append([t['email'], t['clients'][0]['sent'], t['clients'][0]['opened'],
                     t['clients'][0]['clicked'], t['clients'][0]['spam'], t['clients'][0]['unsub'],
                     t['clients'][0]['softbounced'], t['clients'][0]['hardbounced']])

    df = pd.DataFrame(data, columns=['email', 'sent', 'opened', 'clicked', 'spam', 'unsub', 'softbounced', 'hardbounced'])
    return df



