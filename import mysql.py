import mysql.connector
import re


# 'hummingbird', 'hummingbird_staging', 'hummingbird_datavalidation'

# ---------- CONFIGURATION ----------
db_config = {
    'host': 'hummingbird-anytimestorage-prod.crn4oqw9fes3.us-east-1.rds.amazonaws.com',
    'port': 3306,
    'user': 'obt_user',
    'password': 'E10PtoRVnlJbzCfC1',
    'database': 'hummingbird'
}
# -----------------------------------

# Connect to database
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

print(f"\n Starting cleanup of DEFINER clauses in database: {db_config['database']}")

try:
    # Fetch all procedures
    cursor.execute(f"SHOW PROCEDURE STATUS WHERE Db = '{db_config['database']}'")
    procedures = cursor.fetchall()

    procedures_list = []

    for proce in procedures:
        proc_name = proce[1]
        procedures_list.append(proc_name)

    print(procedures_list)

    if not procedures:
        print("⚠️ No stored procedures found.")
    else:
        for proc in procedures:
            proc_name = proc[1]
            print(f"  → Processing procedure: {proc_name}")

            try:
                # Fetch CREATE statement
                cursor.execute(f"SHOW CREATE PROCEDURE `{db_config['database']}`.`{proc_name}`")
                row = cursor.fetchone()

                # Make sure we got a valid result
                if not row or len(row) < 3 or row[2] is None:
                    print(f"⚠️ Could not fetch CREATE statement for '{proc_name}', skipping...")
                    continue

                create_stmt = row[2]

                # Make sure it's a string before using regex
                if isinstance(create_stmt, str):
                    # Remove DEFINER clause
                    create_stmt = re.sub(r"DEFINER\s*=\s*`[^`]+`@`[^`]+`\s*", "", create_stmt, flags=re.IGNORECASE)

                    # # Optional: remove MySQL versioned comments
                    # create_stmt = re.sub(r"/\*![0-9]+ .*?\*/", "", create_stmt, flags=re.DOTALL)
                    #
                    # # Optional: remove SQL_MODE or ENGINE directives
                    # create_stmt = re.sub(r"SQL_MODE=.*?;", "", create_stmt, flags=re.IGNORECASE)
                    # create_stmt = re.sub(r"ENGINE=.*?;", "", create_stmt, flags=re.IGNORECASE)

                    # Optional: normalize CREATE keyword
                    create_stmt = re.sub(r"CREATE\s+.*?\s+PROCEDURE", "CREATE PROCEDURE", create_stmt,
                                         flags=re.IGNORECASE | re.DOTALL)
                else:
                    print(f"⚠️ CREATE statement for '{proc_name}' is not a string, skipping...")
                    continue

                # Drop old procedure safely
                try:
                    cursor.execute(f"DROP PROCEDURE IF EXISTS `{proc_name}`")
                except mysql.connector.Error as err:
                    print(f"⚠️ Failed to drop procedure '{proc_name}': {err}, skipping DROP")

                # Recreate procedure safely
                try:
                    cursor.execute(create_stmt)
                    print(f"✅ Recreated: {proc_name}")
                except mysql.connector.Error as err:
                    print(f"❌ Failed to recreate '{proc_name}': {err}, skipping this procedure")
                    continue


            except mysql.connector.Error as err:
                print(f"⚠️ Error fetching definition for '{proc_name}': {err}")
                continue

        conn.commit()
        print("\n✅ All stored procedures processed successfully.")

except mysql.connector.Error as err:
    print(f"❌ Failed to list procedures: {err}")


#
# try:
#     cursor.execute(f"SHOW FUNCTION STATUS WHERE Db = '{db_config['database']}'")
#     functions = cursor.fetchall()
#
#     for func in functions:
#         func_name = func[1]
#         print(f"  → Function: {func_name}")
#
#         try:
#             # Fetch CREATE statement
#             cursor.execute(f"SHOW CREATE FUNCTION `{db_config['database']}`.`{func_name}`")
#             row = cursor.fetchone()
#             if not row or len(row) < 3:
#                 print(f"⚠️ Could not retrieve CREATE statement for function '{func_name}', skipping...")
#                 continue
#
#             create_stmt = row[2]
#
#             # Remove DEFINER clause (for any user@host)
#             create_stmt = re.sub(r"CREATE\s+.*?\s+function", "CREATE function", create_stmt,
#                                 flags=re.IGNORECASE)
#
#
#
#
#             # Drop existing function on target
#             cursor.execute(f"DROP FUNCTION IF EXISTS `{func_name}`")
#
#             # Try creating the function
#             try:
#                 cursor.execute(create_stmt)
#             except mysql.connector.Error as err:
#                 print(f"❌ Failed to create function '{func_name}': {err}")
#                 print("↳ Skipping this function...\n")
#                 continue
#
#         except mysql.connector.Error as err:
#             print(f"⚠️ Error fetching definition for function '{func_name}': {err}")
#             continue
#
#     # Commit all successful creations at the end
#     # cursor.commit()
#     print("\n✅ Done copying functions.")
#
# except mysql.connector.Error as err:
#     print(f"❌ Failed to fetch function list: {err}")

# --------------------------------------------------------------------
# 3️⃣ Copy Triggers
# --------------------------------------------------------------------
# print("\n🧩 Copying triggers...")
# cursor.execute(f"SHOW TRIGGERS FROM `{db_config['database']}`")
# triggers = cursor.fetchall()
#
# for trigger in triggers:
#     trigger_name = trigger[0]
#     print(f"  → Trigger: {trigger_name}")
#     cursor.execute(f"SHOW CREATE TRIGGER `{db_config['database']}`.`{trigger_name}`")
#     create_stmt = cursor.fetchone()[2]
#     create_stmt = re.sub(r"CREATE\s+.*?\s+trigger", "CREATE trigger", create_stmt,
#                          flags=re.IGNORECASE)
#     cursor.execute(f"DROP TRIGGER IF EXISTS `{trigger_name}`")
#     cursor.execute(create_stmt)

# Cleanup
cursor.close()
conn.close()