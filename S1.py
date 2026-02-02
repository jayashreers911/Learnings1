import mysql.connector

connection=mysql.connector.connect(
    host='hummingbird-trialmigration-prod.crn4oqw9fes3.us-east-1.rds.amazonaws.com',
    user='jayashree',
    password='3cphI1Yk5VD7rDdi',
    database='hummingbird'
)

cursor=connection.cursor(dictionary=True)

query="select * from companies limit 10"

cursor.execute(query)

results=cursor.fetchall()

for row in results:
    print(row["column_name"])

cursor.close()

connection.close()