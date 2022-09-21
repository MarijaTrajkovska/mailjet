from fastapi import APIRouter, File, UploadFile
from fastapi.responses import RedirectResponse
from fastapi.responses import FileResponse
import starlette.status as status
from partnership_fastapi.app.utils import zip_files, save_uploaded_file
from typing import Union
from partnership_fastapi.app.mongo_api import MongoApi
from dotenv import load_dotenv
import os

load_dotenv(r"env/.env.local")
EMAIL_SELECTION_UPLOADED_DIR = os.getenv('EMAIL_SELECTION_UPLOADED_DIR')
EMAIL_SELECTION_RETURN_DIR = os.getenv('EMAIL_SELECTION_RETURN_DIR')

router = APIRouter(prefix="/email_selection")

mongo_api = MongoApi(host="mongodb://dataScience:20PostoPopust.@65.21.194.47:27010/")

all_clients = ['FemPeak', 'Mobile Monkey', 'Pico', 'Laylo', 'URUE', 'OnScale',
               'CreatorStack', 'Ashleigh', 'GoSuper', 'Bonfire', 'PearPop',
               'Muse', 'Fangage', 'Zezam', 'Cloutart', 'Off Script', 'Credd',
               'Pietra', 'Backstage', 'Mavely', 'Pillar', 'Podz', 'Mayk',
               'Maestro.io', 'Noodle Soup', 'Willa', 'Influencers Newsletter',
               'Fanstories', 'Wiire', 'BREEZE', 'Reach.me', 'Lili.co', 'Cura',
               'Creable', 'Pensight', 'Gelato', 'BigRoomTV', 'RoboBDR', 'GWARM',
               'Curacity', 'HP', 'foodbygracja']


@router.get("/files/download",  include_in_schema=False)
async def get_best_and_worst_emails():
    file_list = [EMAIL_SELECTION_RETURN_DIR + '/best_emails_file.csv',
                 EMAIL_SELECTION_RETURN_DIR + '/spam_unsub_hb_emails_file.csv']
    return zip_files(file_list)


@router.get("/get_spam_unsub_hardbounce",  include_in_schema=False)
async def get_worst_emails():
    response = FileResponse(path=EMAIL_SELECTION_RETURN_DIR + '/spam_unsub_hb_emails_file.csv',
                            filename="spam_unsub_hb_emails_file", media_type='text/csv')
    return response


@router.get("/get_best_res",  include_in_schema=False)
async def get_best_emails():
    response = FileResponse(path=EMAIL_SELECTION_RETURN_DIR + '/best_emails_file.csv',
                            filename="best_emails_file", media_type='text/csv')
    return response


@router.post("/best_res___spam_unsub_hb", tags=["Get both good and bad scored emails"])
async def get_good_and_bad_emails(upload_file: UploadFile = File(...), client: Union[str, None] = None):
    if client is not None and client not in all_clients:
        return "Klientot sto go vnesovte momentalno ne e vo baza!"

    uploaded_file_path = os.path.join(EMAIL_SELECTION_UPLOADED_DIR, upload_file.filename)
    save_uploaded_file(uploaded_file_path, upload_file)

    if client in all_clients:
        best_emails_df, spam_unsub_hb_emails_df = mongo_api.filter_emails_with_client(uploaded_file_path, client, "both")
    elif client is None:
        best_emails_df, spam_unsub_hb_emails_df = mongo_api.filter_emails(uploaded_file_path, "both")
    else:
        return "Neopfanato scenario :')"

    best_emails_df.to_csv(EMAIL_SELECTION_RETURN_DIR + "/best_emails_file.csv", index=False)
    spam_unsub_hb_emails_df.to_csv(EMAIL_SELECTION_RETURN_DIR + "/spam_unsub_hb_emails_file.csv", index=False)

    response = RedirectResponse('/files/download', status_code=status.HTTP_303_SEE_OTHER)
    return response


@router.post("/best_res", tags=["Get good scored emails only"])
async def get_good_emails(upload_file: Union[UploadFile, None] = None, client: Union[str, None] = None):
    if upload_file is None and client is None:
        return "Potrebno e da vnesete vrednost barem za edno od polinjata!"

    if client is not None and client not in all_clients:
        return "Klientot sto go vnesovte momentalno ne e vo baza!"

    if upload_file is not None:
        uploaded_file_path = os.path.join(EMAIL_SELECTION_UPLOADED_DIR, upload_file.filename)
        save_uploaded_file(uploaded_file_path, upload_file)

        if client is None:
            best_emails_df = mongo_api.filter_emails(uploaded_file_path, "good")
        else:
            best_emails_df = mongo_api.filter_emails_with_client(uploaded_file_path, client, "good")

    else:
        best_emails_df = mongo_api.get_good_emails_statistics_for_client(client)

    best_emails_df.to_csv(EMAIL_SELECTION_RETURN_DIR + "/best_emails_file.csv", index=False)

    response = RedirectResponse('/get_best_res', status_code=status.HTTP_303_SEE_OTHER)
    return response


@router.post("/spam_unsub_hb", tags=["Get bad scored emails only"])
async def get_bad_emails(upload_file: Union[UploadFile, None] = None, client: Union[str, None] = None):
    if upload_file is None and client is None:
        return "Potrebno e da vnesete vrednost barem za edno od polinjata!"
    if client is not None and client not in all_clients:
        return "Klientot sto go vnesovte momentalno ne e vo baza!"

    if upload_file is not None:
        uploaded_file_path = os.path.join(EMAIL_SELECTION_UPLOADED_DIR, upload_file.filename)
        save_uploaded_file(uploaded_file_path, upload_file)

        if client is None:
            spam_unsub_hb_emails_df = mongo_api.filter_emails(uploaded_file_path, "bad")
        else:
            spam_unsub_hb_emails_df = mongo_api.filter_emails_with_client(uploaded_file_path, client, "bad")

    else:
        spam_unsub_hb_emails_df = mongo_api.get_bad_emails_statistics_for_client(client)

    spam_unsub_hb_emails_df.to_csv(EMAIL_SELECTION_RETURN_DIR + "/spam_unsub_hb_emails_file.csv", index=False)

    response = RedirectResponse('/get_spam_unsub_hardbounce', status_code=status.HTTP_303_SEE_OTHER)
    return response

