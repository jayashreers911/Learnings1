import mysql.connector
from mysql.connector import Error
import pandas as pd

try:
    connection=mysql.connector.connect(
        host="hummingbird-trialmigration-prod.crn4oqw9fes3.us-east-1.rds.amazonaws.com",
        user="jayashree",
        password="3cphI1Yk5VD7rDdi",
        database="hummingbird_staging"
    )

    if connection.is_connected():
        print("Connection successful")
        num=5;
        print("num is",num)

        cursor=connection.cursor()
        # query=f"select * from hummingbird_staging.nw_Units_All limit {num};"
        query=f"select table_name from information_schema.columns where column_name  ='property_id' and table_schema='hummingbird';"
        print(query)
        cursor.execute(query)
        results=cursor.fetchall()

        for row in results:
            print(row)
        df=pd.read_sql(query,connection)
        print(df.info())
        print(df.head())
        print(df.fillna("NA"))
        # print(df[['Owner','Space']])
        # print(df.filter(like='name'))
        print(df.columns)

except Error as e:
    print("error is ",e)

finally:
    try:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("connection closed")
    except NameError:
        print(NameError)

