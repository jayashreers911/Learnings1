import mysql.connector
from mysql.connector import Error
import  pandas as pd

print("starting")

try:
    connection=mysql.connector.connect(
        host="hummingbird-trialmigration-prod.crn4oqw9fes3.us-east-1.rds.amazonaws.com",
        user="jayashree",
        password="3cphI1Yk5VD7rDdi",
        database="hummingbird_staging"
    )

    if connection.is_connected():
        print("Connection successful")

        num=10;
        cursor=connection.cursor()
        query1="select * from nw_Units_All limit 5;"
        query2="select * from nw_Units_All limit 10;"
        cursor.execute(query1)
        results1=cursor.fetchall()
        cursor.execute(query2)
        results2=cursor.fetchall()
        print("checking results")

        # if results1==results2:
        #     print("matching results")
        # else:
        #     print("output doesnt match")

        df1=pd.read_sql(query1,connection)
        df2=pd.read_sql(query2,connection)

        # print("Head is",df1.head())
        # print("Info is",df1.info())
        # print("desc is",df1.describe())
        # print("cols is",df1.columns)

        # df1_rename=df1.rename(columns={"Owner":"Company_Name"})
        # print("DF1 is ",df1_rename)

        # df1_df2_merged=pd.merge(df1,df1,on="Building",how="inner")
        # print("merged is ",df1_df2_merged)
        print(df1.iloc[0])
        print(df1.loc[0,"Owner"])

        # print(df1[["Owner","Name"]])


        
        # print(f"Fetched {len(results)} rows")
        # for row in results:
        #     print(row)

except Error as e:
    print("Error is",e)

finally:
    try:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print ("Connection closed")
    except NameError:
        print("connection was never created")
