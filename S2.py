import mysql.connector
import pandas as pd

connection=mysql.connector.connect(
    host='hummingbird-trialmigration-prod.crn4oqw9fes3.us-east-1.rds.amazonaws.com',
    user='jayashree',
    password='3cphI1Yk5VD7rDdi',
    database='hummingbird'
)

query="select * from companies limit 10"

df=pd.read_sql(query,connection)

print(df.head())

connection.close()

