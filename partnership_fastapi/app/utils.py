import zipfile
from io import BytesIO
from starlette.responses import StreamingResponse
import pandas as pd
import shutil
from dotenv import load_dotenv
import os

load_dotenv(r"env/.env.local")
CSV_FILES_RETURN_DIR = os.getenv('CSV_FILES_RETURN_DIR')


def zip_files(file_list):
    io = BytesIO()
    zip_sub_dir = "final_archive"
    zip_filename = "%s.zip" % zip_sub_dir
    with zipfile.ZipFile(io, mode='w', compression=zipfile.ZIP_DEFLATED) as zip:
        for fpath in file_list:
            zip.write(fpath)
        zip.close()
    return StreamingResponse(
        iter([io.getvalue()]),
        media_type="application/x-zip-compressed",
        headers={"Content-Disposition": f"attachment;filename=%s" % zip_filename}
    )


def save_uploaded_file(uploaded_file_path, uploaded_file):
    try:
        with open(uploaded_file_path, "wb") as buffer:
            shutil.copyfileobj(uploaded_file.file, buffer)
    finally:
        uploaded_file.file.close()


# ####################################################################
# ###################  COMPARING TWO FILES ###########################
# ####################################################################


def compare_two_files(file_one, file_two, compare_column):
    file_one = pd.read_csv(file_one)
    file_two = pd.read_csv(file_two)
    comp_column_f2 = list(file_two[compare_column])
    only_in_file_one = file_one[~file_one[compare_column].isin(comp_column_f2)]
    return_file_path = os.path.join(CSV_FILES_RETURN_DIR, "final_csv_compared.csv")
    only_in_file_one.to_csv(return_file_path, index=False)


# ####################################################################
# ###################  REMOVE DUPLICATES #############################
# ####################################################################


def get_df_without_duplicates_combined(df, search_columns):
    df.drop_duplicates(subset=search_columns, inplace=True)
    return_file_path = os.path.join(CSV_FILES_RETURN_DIR, "duplicates_removed.csv")
    df.to_csv(return_file_path, index=False)


def get_df_without_duplicates_separately(df, search_columns):
    for column in search_columns:
        df.drop_duplicates(subset=[column], inplace=True)
    return_file_path = os.path.join(CSV_FILES_RETURN_DIR, "duplicates_removed.csv")
    df.to_csv(return_file_path, index=False)


# ####################################################################
# ################### MERGE TWO CSV FILES ############################
# ####################################################################


def get_merged_files(file_path_one, file_path_two):
    df1 = pd.read_csv(file_path_one)
    df2 = pd.read_csv(file_path_two)
    merged = pd.concat([df1, df2], ignore_index=True)
    return_file_path = os.path.join(CSV_FILES_RETURN_DIR, "merged_csv_file.csv")
    merged.to_csv(return_file_path, index=False)


# ####################################################################
# ################# INTERSECTION OF TWO CSV FILES ####################
# ####################################################################


def get_intersection_of_files(file_path_one, file_path_two, intersection_column):
    df1 = pd.read_csv(file_path_one)
    df2 = pd.read_csv(file_path_two)
    intersec_column_df2 = list(df2[intersection_column])
    intersection = df1[df1[intersection_column].isin(intersec_column_df2)]
    return_file_path = os.path.join(CSV_FILES_RETURN_DIR, "intersection_of_files.csv")
    intersection.to_csv(return_file_path, index=False)
