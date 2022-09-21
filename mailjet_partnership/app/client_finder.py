from tqdm import tqdm
import re


def find_client_from_subject(subject):
    haRegex = re.compile(r'(re:)*\s*@*[a-z]*.*\s@*[a-z]*')
    try:
        subject = subject.lower()
        if subject.find("culture consultant") != -1:
            return "RoboBDR"
        elif subject.find("creator program") != -1 or subject.find("take part in our beta") != -1 or subject.find(
                "5k investment") != -1 or subject.find("credd") != -1 or subject.find(
            "revenue all in one dashboard") != -1:
            return "Credd"
        elif subject.find("zezam") != -1 or subject.find("klitschko") != -1 or subject.find("why go to the moon") != -1:
            return "Zezam"
        elif subject.find("pico") != -1:
            return "Pico"
        elif subject.find("fempeak") != -1:
            return "FemPeak"
        elif subject.find(" roi ") != -1 or subject.find("7x your ") != -1 \
                or subject.find("chat messaging") != -1 or subject.find("chat automation") != -1 \
                or subject.find("mailchimp") != -1 or subject.find("hubspot") != -1:
            return "Mobile Monkey"
        elif subject.find("sms ") != -1 or subject.find("salesforce") != -1 \
                or subject.find("is looking for") != -1 or subject.find("g2-approved ") != -1 \
                or subject.find("boycott manychat") != -1 or subject.find("blue ig checkmark") != -1:
            return "Mobile Monkey"
        elif subject.find("laylo") != -1 or subject.find("sony podcast") != -1:
            return "Laylo"
        elif subject.find("urue") != -1 or subject.find("powerful brand") != -1:
            return "URUE"
        elif subject.find("highest paid creators' group chat") != -1 or subject.find("onscale") != -1:
            return "OnScale"
        elif subject.find("from your social circles") != -1 or subject.find(
                "we connect you with investors") != -1 or subject.find("people want to invest in you") != -1:
            return "GoSuper"
        elif subject.find("@jakepaul") != -1 or subject.find("@snoopdog") != -1 \
                or subject.find("@shawnmendes") != -1 or subject.find("@shaq") != -1 \
                or subject.find("pearpop") != -1 or subject.find("panera") != -1 or subject.find("sips") != -1:
            return "PearPop"
        elif subject.find("fangage") != -1:
            return "Fangage"
        elif subject.find("bonfire") != -1 or subject.find("about your link in bio") != -1:
            return "Bonfire"
        elif subject.find("muse") != -1 or subject.find("irs new rules") != -1:
            return "Muse"
        elif subject.find("mavely") != -1:
            return "Mavely"
        elif subject.find("pillar") != -1:
            return "Pillar"
        elif subject.find("creatorstack") != -1:
            return "CreatorStack"
        elif subject.find("can we connect on partnerships + growth") != -1:
            return "Ashleigh"
        elif subject.find("own brand") != -1 or subject.find("ownbrand") != -1 \
                or subject.find("workshop comming") != -1 or subject.find("workshop coming") != -1 \
                or subject.find("pietra") != -1 or subject.find("want to start the next") != -1:
            return "Pietra"
        elif subject.find("quick question about your fashion store") != -1 \
                or subject.find("your e-comm brand") != -1 or subject.find("will smith & robert downey jr") != -1 \
                or subject.find("launch your first product") != -1 or subject.find("promo code") != -1:
            return "Pietra"
        elif subject.find("nft") != -1:
            return "Cloutart"
        elif subject.find("your own ig shop") != -1 or subject.find("team that builds a custom") != -1 \
                or subject.find("hvisk") != -1 or subject.find("silfenstudio") != -1 \
                or subject.find("sweedbeauty") != -1 or subject.find("theofficialsafira") != -1:
            return "Off Script"
        elif subject.find("understatement_underwear") != -1 or subject.find("off script") != -1 \
                or subject.find("live your passion") != -1 or subject.find("instagram shopping a shot") != -1:
            return "Off Script"
        elif subject.find("build your own multi-brand") != -1 or subject.find("ig shopping") != -1 \
                or subject.find("what brand did you wear") != -1 or subject.find("into a shopping cart") != -1 \
                or haRegex.search(subject) is not None:
            return "Off Script"
        elif subject.find("backstage") != -1:
            return "Backstage"
        elif subject.find("pillar") != -1 or subject.find("ninja") != -1:
            return "Pillar"
        elif subject.find("podz") != -1 or subject.find("fanz") != -1:
            return "Podz"
        elif subject.find("willa") != -1:
            return "Willa"
        elif subject.find("wiire ") != -1 or subject.find("are you sick of waiting for") != -1:
            return "Wiire"
        elif subject.find("fanstories") != -1:
            return "Fanstories"
        elif subject.find("news & opportunities await") != -1 \
                or subject.find("see what's been going on this week") != -1 \
                or subject.find("new opportunities await") != -1 or subject.find("step up your content game") != -1 \
                or subject.find("update you on creator things") != -1 \
                or subject.find("weekly handpicked collab") != -1:
            return "Influencers Newsletter"
        elif subject.find("moment house") != -1:
            return "Moment House"
        elif subject.find("redcircle") != -1:
            return "RedCircle"
        elif subject.find("curacity") != -1:
            return "Curacity"
        elif subject.find("pensight") != -1 or subject.find("pensigh") != -1:
            return "Pensight"
        elif subject.find("creable") != -1:
            return "Creable"
        elif subject.find("hp book partnership") != -1:
            return "HP"
        elif subject.find("warner records") != -1 or subject.find("reach.me") != -1:
            return "Reach.me"
        elif subject.find("mayk") != -1 or subject.find("promoting your music") != -1:
            return "Mayk"
        elif subject.find("breeze") != -1 or subject.find("back catalog") != -1:
            return "BREEZE"
        elif subject.find("creable") != -1:
            return "Creable"
        elif subject.find("creable") != -1:
            return "Creable"
        elif subject.find("liliсо ") != -1 or subject.find("lili.co") != -1 or subject.find("want 60 hours more") != -1:
            return "Lili.co"
        elif subject.find("own food product") != -1:
            return "Cura"
        elif subject.find("the plug") != -1:
            return "Jet Fuel"
        elif subject.find("noodle") != -1:
            return "Noodle Soup"
        elif subject.find("hibeam") != -1:
            return "Hi Beam"
        elif subject.find("better content syndication") != -1 or subject.find("podcast on tiktok and insta") != -1:
            return "BigRoomTV"
        elif subject.find("guidereply") != -1:
            return "Guidereply"
        elif subject.find("gelato") != -1:
            return "Gelato"
        elif subject.find("maestro") != -1:
            return "Maestro.io"
        elif subject.find("warm, sweet concoction!") != -1:
            return "GWARM"
        elif subject.find("foodbygracja") != -1 or subject.find("nie przegap promocji") != -1:
            return "foodbygracja"
        else:
            return "Other Client"

    except AttributeError:
        print("The subject is: ", subject)
        return None


def find_clients_from_df(df):
    for index, row in tqdm(df.iterrows(), total=df.shape[0]):
        about = find_client_from_subject(row['subject'])
        df.at[index, 'email_about'] = about
    return df
