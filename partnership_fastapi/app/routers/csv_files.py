from fastapi import APIRouter, File, UploadFile, Query
from fastapi.responses import RedirectResponse
from fastapi.responses import FileResponse
import starlette.status as status
from partnership_fastapi.app.utils import compare_two_files, get_df_without_duplicates_combined, save_uploaded_file, \
    get_df_without_duplicates_separately, get_merged_files, get_intersection_of_files
import pandas as pd
import re
from dotenv import load_dotenv
import os

load_dotenv(r"env/.env.local")
CSV_FILES_RETURN_DIR = os.getenv('CSV_FILES_RETURN_DIR')
CSV_FILES_UPLOADED_DIR = os.getenv('CSV_FILES_UPLOADED_DIR')

router = APIRouter(prefix="/csv_files")

# ########################## GET METHODS ##########################

@router.get("/download_compared_files", include_in_schema=False)
async def download_compared_files():
    response = FileResponse(path=CSV_FILES_RETURN_DIR + '/final_csv_compared.csv',
                            filename="final_csv_compared", media_type='text/csv')
    return response


@router.get("/download_file_without_duplicates", include_in_schema=False)
async def download_file_without_duplicates():
    response = FileResponse(path=CSV_FILES_RETURN_DIR + '/duplicates_removed.csv',
                            filename="duplicates_removed", media_type='text/csv')
    return response


@router.get("/download_merged_csv_file", include_in_schema=False)
async def download_merged_csv_file():
    response = FileResponse(path=CSV_FILES_RETURN_DIR + '/merged_csv_file.csv',
                            filename="merged_csv_file", media_type='text/csv')
    return response


@router.get("/download_intersection", include_in_schema=False)
async def download_intersection_of_files():
    response = FileResponse(path=CSV_FILES_RETURN_DIR + '/intersection_of_files.csv',
                            filename="intersection_of_files", media_type='text/csv')
    return response


# ########################## POST METHODS ##########################


@router.post("/compare_csv_files", tags=['Compare two .csv files (File1 - File2)'])
async def compare_csv_files(upload_file_one: UploadFile = File(...), upload_file_two: UploadFile = File(...),
                            compare_by_column: str = Query(default="username")):

    uploaded_file_path_one = os.path.join(CSV_FILES_UPLOADED_DIR, upload_file_one.filename)
    save_uploaded_file(uploaded_file_path_one, upload_file_one)

    uploaded_file_path_two = os.path.join(CSV_FILES_UPLOADED_DIR, upload_file_two.filename)
    save_uploaded_file(uploaded_file_path_two, upload_file_two)

    df1 = pd.read_csv(uploaded_file_path_one)
    all_columns_df1 = list(df1.columns.values)
    df2 = pd.read_csv(uploaded_file_path_two)
    all_columns_df2 = list(df2.columns.values)

    if compare_by_column not in all_columns_df1 and compare_by_column not in all_columns_df2:
        return "I vo dvata prikaceni fajlovi, ne postoi kolona so ime: " + compare_by_column
    elif compare_by_column not in all_columns_df1:
        return "Vo prviot prikacen fajl ne postoi kolona so ime: " + compare_by_column
    elif compare_by_column not in all_columns_df2:
        return "Vo vtoriot prikacen fajl ne postoi kolona so ime: " + compare_by_column

    compare_two_files(uploaded_file_path_one, uploaded_file_path_two, compare_by_column)

    response = RedirectResponse('/download_compared_files', status_code=status.HTTP_303_SEE_OTHER)
    return response


@router.post("/remove_duplicates_upload_file", tags=['Remove duplicates from a .csv file'])
async def remove_duplicates_upload_file(remove_duplicates_separately: bool,
                                        search_column: str = Query(default="username"),
                                        upload_file: UploadFile = File(...)):

    uploaded_file_path = os.path.join(CSV_FILES_UPLOADED_DIR, upload_file.filename)
    save_uploaded_file(uploaded_file_path, upload_file)

    df = pd.read_csv(uploaded_file_path)
    all_columns_df = list(df.columns.values)
    search_columns = re.split(" *, *", search_column)

    if set(search_columns).issubset(set(all_columns_df)) is False:
        return "Iminjata na vnesenite koloni ne se poklopuvaat so iminjata na kolonite vo fajlot!"

    if remove_duplicates_separately is False:
        get_df_without_duplicates_combined(df, search_columns)

    else:
        get_df_without_duplicates_separately(df, search_columns)

    response = RedirectResponse('/download_file_without_duplicates', status_code=status.HTTP_303_SEE_OTHER)
    return response


@router.post("/merge_two_csv_files", tags=['Merge two .csv files'])
async def merge_two_csv_files(upload_file_one: UploadFile = File(...), upload_file_two: UploadFile = File(...)):

    uploaded_file_path_one = os.path.join(CSV_FILES_UPLOADED_DIR, upload_file_one.filename)
    save_uploaded_file(uploaded_file_path_one, upload_file_one)

    uploaded_file_path_two = os.path.join(CSV_FILES_UPLOADED_DIR, upload_file_two.filename)
    save_uploaded_file(uploaded_file_path_two, upload_file_two)

    get_merged_files(uploaded_file_path_one, uploaded_file_path_two)

    response = RedirectResponse('/download_merged_csv_file', status_code=status.HTTP_303_SEE_OTHER)
    return response


@router.post("/intersection_of_two_csv_files", tags=['Intersection of two .csv files'])
async def intersection_of_files(upload_file_one: UploadFile = File(...), upload_file_two: UploadFile = File(...),
                                intersection_column: str = Query(default="username")):

    uploaded_file_path_one = os.path.join(CSV_FILES_UPLOADED_DIR, upload_file_one.filename)
    save_uploaded_file(uploaded_file_path_one, upload_file_one)

    uploaded_file_path_two = os.path.join(CSV_FILES_UPLOADED_DIR, upload_file_two.filename)
    save_uploaded_file(uploaded_file_path_two, upload_file_two)

    df1 = pd.read_csv(uploaded_file_path_one)
    all_columns_df1 = list(df1.columns.values)
    df2 = pd.read_csv(uploaded_file_path_two)
    all_columns_df2 = list(df2.columns.values)

    if intersection_column not in all_columns_df1 and intersection_column not in all_columns_df2:
        return "I vo dvata prikaceni fajlovi, ne postoi kolona so ime: " + intersection_column
    elif intersection_column not in all_columns_df1:
        return "Vo prviot prikacen fajl ne postoi kolona so ime: " + intersection_column
    elif intersection_column not in all_columns_df2:
        return "Vo vtoriot prikacen fajl ne postoi kolona so ime: " + intersection_column

    get_intersection_of_files(uploaded_file_path_one, uploaded_file_path_two, intersection_column)

    response = RedirectResponse('/download_intersection', status_code=status.HTTP_303_SEE_OTHER)
    return response

