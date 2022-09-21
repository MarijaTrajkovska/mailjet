from fastapi import FastAPI
from routers import csv_files, email_selection
import uvicorn


tags_metadata = [
    {
        "name": "Compare two .csv files (File1 - File2)",
        "description": "Upload two .csv files and write down the name of the column you want to use for comparing. "
                       "Make sure that both .csv files have the **exact same name** for the column used for comparing "
                       "the files. "
                       "**If the column's name in the first file is 'username', and in the second file is 'Username', "
                       "the compare task won't work.**",
    },
    {
        "name": "Remove duplicates from a .csv file",
        "description": "Upload a .csv file and write down the name of the column/columns you want to remove the "
                       "duplicates from. "
                       "**_For example_**, if you want to remove duplicates only in one column,"
                       " you can write **_'username'_**, and it will remove the duplicates based on that column. "
                       "If you want to remove duplicates from multiple columns, you can write **_'username, email'_**. "
                       "and it will remove the duplicates based on those columns. For multiple columns, you should "
                       "pay attention on the **input format, it should be like this 'column_name1, column_name2, "
                       "..., column_nameN** ."
                       "The field **_remove_duplicated_separately_** is for whether you want the duplicates from "
                       "multiple columns to be removed separately for each column or to be removed as a pair "
                       "of columns."
    },
    {
        "name": "Merge two .csv files",
        "description": "Upload two .csv files. **Make sure the files have the exact same name for the columns**. "
                       "Note that if you have a column named as **_email_** in the first file and **_Email_** "
                       "in the other, you will get two different columns in the merged file. "

    },
    {
        "name": "Intersection of two .csv files",
        "description": "Upload two .csv files. **Make sure the files have the exact same name for the columns**. "
                       "Write down the name of the column you want to find intersection of."
                       "Note that if you have a column named as **_email_** in the first file and **_Email_** "
                       "in the other, the intersection will not be found. "

    },
{
        "name": "Get both good and bad scored emails",
        "description": "Upload a .csv file and write down the name of the client you want to get results for. "
                       "If you don't need results for a specific client, leave this field blank."

    },
    {
        "name": "Get good scored emails only",
        "description": "Upload a .csv file and write down the name of the client you want to get results for. "
                       "If you don't need results for a specific client, leave this field blank."
                       "If you want to get all good emails for a specific client, without uploading a file, "
                       "write down the name of the client only, without uploading a .csv file."

    },
    {
        "name": "Get bad scored emails only",
        "description": "Upload a .csv file and write down the name of the client you want to get results for. "
                       "If you don't need results for a specific client, leave this field blank."
                       "If you want to get all bad emails for a specific client, without uploading a file, "
                       "write down the name of the client only, without uploading a .csv file."

    }

]
app = FastAPI(title="Partnership Team API 2", openapi_tags=tags_metadata,
              swagger_ui_parameters={"defaultModelsExpandDepth": -1})


app.include_router(csv_files.router)
app.include_router(email_selection.router)


if __name__ == '__main__':
    uvicorn.run("main:app", port=9988, reload=True)