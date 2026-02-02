import pandas as pd
import psycopg2
import os
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
from sqlalchemy.orm import sessionmaker
import mysql.connector
from mysql.connector import Error
import numpy as np
from datetime import datetime, timedelta
from urllib.parse import quote
import logging
import ast
from dotenv import load_dotenv
load_dotenv()


# Function to connect to Redshift
def connect_redshift(username,password,port,redshift_endpoint,database):
    try:
        username = quote(username)
        password = quote(password)
        redshift_engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{redshift_endpoint}:{port}/{database}')
        with redshift_engine.connect() as connection:
            print("Redshift connection successful")
            return redshift_engine
    except Exception as e:
        print(f"Error connecting to Redshift: {e}")
        return None

# Function to connect to PostGre on RDS
def connect_postgresql(username,password,postgre_endpoint,database):
    try:
        postgre_username = quote(username)
        postgre_password = quote(password)

        #mysql_engine = create_engine(f'mysql+mysqlconnector://{username}:{password}@{rds_endpoint}:3306/{database}')
        postgre_engine = create_engine(f'postgresql+psycopg2://{postgre_username}:{postgre_password}@{postgre_endpoint}:5432/{database}')
        with postgre_engine.connect() as connection:
            print("Postgre connection successful")
        return postgre_engine
    except Exception as e:
        print(str(e))
        print(f"Error connecting to Postgre server: {e}")
        return None
    
def connect_mysql(username,password,rds_endpoint,database):
    try:
        rds_username = quote(username)
        rds_password = quote(password)

        #mysql_engine = create_engine(f'mysql+mysqlconnector://{username}:{password}@{rds_endpoint}:3306/{database}')
        mysql_engine = create_engine(f'mysql+mysqlconnector://{rds_username}:{rds_password}@{rds_endpoint}:3306/{database}')
        with mysql_engine.connect() as connection:
            print("MySQL connection successful")
        return mysql_engine
    except Exception as e:
        print(str(e))
        print(f"Error connecting to RDS server: {e}")
        return None


# Function to fetch data from a database
def fetch_data(engine, query):
    with engine.connect() as connection:
        print(query)
        data = pd.read_sql(query, connection)
    return data

# Function to execute multiple statements for MySQL
def execute_mysql_statements(engine, query):
    results = []
    try:
        with engine.connect() as connection:
            statements = query.split(';')
            for statement in statements:
                if statement.strip():
                    connection.execute(text(statement))
                    if "SELECT" in statement:
                        results.append(pd.read_sql(statement, connection))
        
        results = pd.concat(results, ignore_index=True)
        return results
    except Exception as e:
        print(f"Error executing MySQL statements: {e}")
        return results
    
def execute_mysql_statements_complimentary(engine, query):
    results = []
    try:
        with engine.connect() as connection:
            statements = query.split(';')
            for statement in statements:
                stmt = statement.strip()
                if not stmt:
                    continue

                # Detect SELECTs safely (case-insensitive)
                if stmt.strip().lower().startswith("select"):
                    df = pd.read_sql(stmt, connection)
                    results.append(df)
                else:
                    connection.execute(text(stmt))

        # Ensure we return a DataFrame
        if results:
            return pd.concat(results, ignore_index=True)
        else:
            return pd.DataFrame()  # Empty DataFrame if no SELECT

    except Exception as e:
        print(f"Error executing MySQL statements: {e}")
        return pd.DataFrame() 
    

prop_sk = ast.literal_eval(os.getenv("property_sk", "[]"))
prop_id = ast.literal_eval(os.getenv("property_id", "[]"))
property_sk = ",".join(str(i) for i in prop_sk)
property_id = ",".join(str(i) for i in prop_id) 

def setDate_Y_M_D(tdw_date):
  adjusted_end_date = pd.to_datetime(tdw_date)
  # adjusted_end_date = pd.to_datetime(tdw_date) - pd.Timedelta(days=1)
  last_day_of_month = adjusted_end_date + pd.offsets.MonthEnd(0)
  # print(adjusted_end_date)
  one_year_back_date = last_day_of_month - pd.DateOffset(years=1) - pd.DateOffset(months=1)
  # print(one_year_back_date)
  first_day_of_month = one_year_back_date +  pd.offsets.MonthBegin(0)
  # print(first_day_of_month)
  # print(last_day_of_month)
  adjusted_end_date = adjusted_end_date.strftime('%Y-%m-%d')
  last_day_of_month = last_day_of_month.strftime('%Y-%m-%d')
  first_day_of_month = first_day_of_month.strftime('%Y-%m-%d')
  return adjusted_end_date,first_day_of_month 

def setDate_Y_M_D_for_PPL(tdw_date):
  adjusted_end_date = pd.to_datetime(tdw_date)
  # adjusted_end_date = pd.to_datetime(tdw_date) - pd.Timedelta(days=1)
  last_day_of_month = adjusted_end_date + pd.offsets.MonthEnd(0)
  # print(adjusted_end_date)
  one_year_back_date = last_day_of_month - pd.DateOffset(years=1) - pd.DateOffset(months=0)
  # print(one_year_back_date)
  first_day_of_month = one_year_back_date +  pd.offsets.MonthBegin(0)
  # print(first_day_of_month)
  # print(last_day_of_month)
  adjusted_end_date = adjusted_end_date.strftime('%Y-%m-%d')
  last_day_of_month = last_day_of_month.strftime('%Y-%m-%d')
  first_day_of_month = first_day_of_month.strftime('%Y-%m-%d')
  return adjusted_end_date,first_day_of_month 

def setDate_For_Ins(tdw_date):
    adjusted_end_date = pd.to_datetime(tdw_date)
    # adjusted_end_date = pd.to_datetime(tdw_date) - pd.Timedelta(days=1)
    last_day_of_month = adjusted_end_date + pd.offsets.MonthEnd(0)
    dates = [adjusted_end_date]

    for i in range(1, 12):
      date = adjusted_end_date - pd.DateOffset(months=i)
      last_day_of_month = date + pd.offsets.MonthEnd(0)
      dates.append(last_day_of_month)
      
    date_strs = [date.strftime('%Y-%m-%d') for date in dates]
    date_conditions = " OR ".join([f"dd.date_dt = '{date_str}'" for date_str in date_strs])
    print(date_conditions)

def execute_mysql_statements_invoices_accural(engine, query):
    results = []
    try:
        with engine.connect() as connection:
            statements = query.split(';')
            for statement in statements:
                if statement.strip():
                    if "SELECT" in statement or "select" in statement:
                        print(statement)
                        result = pd.read_sql(statement, connection)
                        results.append(result)
                    else:
                        connection.execute(text(statement))
        if results:
            results = pd.concat(results, ignore_index=True)
        else:
            results = pd.DataFrame()  # Return an empty DataFrame if no results
        return results
    except Exception as e:
        print(f"Error executing MySQL statements: {e}")
        return pd.DataFrame()

def execute_mysql_statements_credits(engine, query):
    results = []
    try:
        with engine.connect() as connection:
            statements = query.split(';')
            for statement in statements:
                if statement.strip():
                    if "SELECT" in statement.strip().upper():
                        df = pd.read_sql(statement, connection)
                        results.append(df)
                    else:
                        connection.execute(text(statement))
        if results:
            return pd.concat(results, ignore_index=True)
        return pd.DataFrame()  # Return an empty DataFrame if no SELECT statements
    except Exception as e:
        print(f"Error executing MySQL statements: {e}")
        return pd.DataFrame()
    
def convert_to_last_day_of_month(date_str):
    # Split the string to get month and year
    month_str, year_str = date_str.split('-')
    # Remove the dash and create full year
    year = '20' + year_str.strip('-')
    # Create a datetime object for the first day of the month
    date = pd.to_datetime(f"{month_str} {year}", format="%B %Y")
    # Get the last day of the month
    last_day_of_month = date + pd.offsets.MonthEnd(0)
    return last_day_of_month.strftime('%Y-%m-%d')

def setDate_M_D_Y(tdw_date):
  adjusted_end_date = pd.to_datetime(tdw_date)
  # adjusted_end_date = pd.to_datetime(tdw_date) - pd.Timedelta(days=1)
  last_day_of_month = adjusted_end_date + pd.offsets.MonthEnd(0)
  one_year_back_date = last_day_of_month - pd.DateOffset(years=1) - pd.DateOffset(months=1)
  
  first_day_of_month = one_year_back_date -  pd.offsets.MonthBegin(0)
  
  adjusted_end_date = adjusted_end_date.strftime('%m-%d-%Y')
  last_day_of_month = last_day_of_month.strftime('%m-%d-%Y')
  first_day_of_month = first_day_of_month.strftime('%m-%d-%Y')
  return adjusted_end_date,first_day_of_month 

def setDate_For_Occupancy(tdw_date):
    adjusted_end_date = pd.to_datetime(tdw_date)
    # adjusted_end_date = pd.to_datetime(tdw_date) - pd.Timedelta(days=1)
    last_day_of_month = adjusted_end_date + pd.offsets.MonthEnd(0)
    dates = [adjusted_end_date]

    for i in range(1, 13):
      date = adjusted_end_date - pd.DateOffset(months=i)
      last_day_of_month = date + pd.offsets.MonthEnd(0)
      dates.append(last_day_of_month)
      
    date_strs = [date.strftime('%Y-%m-%d') for date in dates]
    date_conditions = " OR ".join([f"dd.date_dt = '{date_str}'" for date_str in date_strs])
    return date_conditions

def setDate_For_Insurance(tdw_date):
    adjusted_end_date = pd.to_datetime(tdw_date)
    # adjusted_end_date = pd.to_datetime(tdw_date) - pd.Timedelta(days=1)
    last_day_of_month = adjusted_end_date + pd.offsets.MonthEnd(0)
    dates = [adjusted_end_date]

    for i in range(1, 15):
      date = adjusted_end_date - pd.DateOffset(months=i)
      last_day_of_month = date + pd.offsets.MonthEnd(0)
      dates.append(last_day_of_month)
      
    date_strs = [date.strftime('%Y-%m-%d') for date in dates]
    date_conditions = " OR ".join([f"dd.date_dt = '{date_str}'" for date_str in date_strs])
    return date_conditions

def connect_postgresql(username,password,postgre_endpoint,database):
    try:
        postgre_username = quote(username)
        postgre_password = quote(password)

        #mysql_engine = create_engine(f'mysql+mysqlconnector://{username}:{password}@{rds_endpoint}:3306/{database}')
        postgre_engine = create_engine(f'postgresql+psycopg2://{postgre_username}:{postgre_password}@{postgre_endpoint}:5432/{database}')
        with postgre_engine.connect() as connection:
            print("Postgre connection successful")
        return postgre_engine
    except Exception as e:
        print(str(e))
        print(f"Error connecting to Postgre server: {e}")
        return None
    
def create_temp_table(redshift_engine):
    temp_query = f"""
    select distinct p.id, p.gds_id
    from dla.properties p
    where p.id in ({property_id})
    """
    temp_df = fetch_data(redshift_engine,temp_query)
    # print(temp_df)
    return temp_df

def User_Reviews(redshift_engine,postgre_engine,df):
    print("User reviews query running")

    df.to_sql('temp_table_redshift',redshift_engine,index=False,if_exists='replace')
    df.to_sql('temp_table_postgre',postgre_engine,index=False,if_exists='replace')

    # temp_df = pd.read_sql("SELECT * FROM temp_table_postgre", postgre_engine)
    # print(temp_df)

    redshift_query = f"""
    select distinct tpdv.src_property_id as property_id, 
  case when trt.type_name = 'google' then 1 
    when trt.type_name = 'yelp' then 2
    when trt.type_name = 'website' then 3 end as type,
    count(turdv.*) as user_reviews_count
    from tdw_ro.tdw_user_reviews_d_v turdv 
    join tdw_ro.tdw_reviews_type_d_v trt on turdv.type_sk = trt.type_sk
    join tdw_ro.tdw_property_d_v tpdv  on turdv.property_sk = tpdv.property_sk 
    join temp_table_redshift ttr on tpdv.src_property_id = ttr.id
    group by 1,2
    order by 2,3 desc, 1 desc;
    """

    postgre_query = f"""
    select distinct ttp.id as property_id,
    case when fr."type" = 'google' then 1 
    when fr."type" = 'yelp' then 2
    when fr."type" = 'website' then 3 end as type, 
    count(ur.*) as user_reviews_count
    from user_review ur 
    join facility_review fr  on ur.facility_review_id  = fr.id 
    join temp_table_postgre ttp on fr.facility_id = ttp.gds_id
    group by 1,2
    order by 2,3 desc, 1 desc;
    """

    redshift_df = fetch_data(redshift_engine, redshift_query)
    postgre_df = fetch_data(postgre_engine, postgre_query)

    # print(postgre_df)

    redshift_grouped = redshift_df
    postgresql_grouped = postgre_df

    

    redshift_grouped = redshift_grouped.add_suffix('_tdw')
    postgresql_grouped = postgresql_grouped.add_suffix('_rds')


    redshift_grouped.rename(columns={'property_id_tdw':'property_id','type_tdw':'type'},inplace=True)
    postgresql_grouped.rename(columns={'property_id_rds':'property_id','type_rds':'type'},inplace=True)
    # print(postgresql_grouped)

    if redshift_df.empty and postgre_df.empty:
        comparison_df = pd.merge(redshift_grouped, postgresql_grouped, on=['property_id','type'], how='outer', indicator=True)
        return comparison_df
    else:
        comparison_df = pd.merge(redshift_grouped, postgresql_grouped, on=['property_id','type'], how='outer', indicator=True)
        # print(comparison_df)
        comparison_df['_merge'] = comparison_df['_merge'].map({'left_only': 'TDW', 'right_only': 'RDS', 'both': 'Both'})

        comparison_df = comparison_df.sort_values(by=['property_id','type'])
    # Determine the status
        comparison_df['Status'] = comparison_df.apply(
        lambda row: 'match' if row['user_reviews_count_tdw'] == row['user_reviews_count_rds'] else 'mismatch', axis=1
        )
        comparison_df = comparison_df[comparison_df['user_reviews_count_tdw'].notna()]
        return comparison_df

def Property_Reviews(redshift_engine,postgre_engine,df):
    print("Property reviews query running")

    df.to_sql('temp_table_redshift',redshift_engine,index=False,if_exists='replace')
    df.to_sql('temp_table_postgre',postgre_engine,index=False,if_exists='replace')

    redshift_query = f"""
    select property_id, round(avg_rating::decimal(10,2),2) as avg_rating, total_reviews from (
    select tpdv.src_property_id as property_id,
    avg(cast(tprpv.total_ratings as float)) as avg_rating,
    cast(sum(reviews_count) as integer) as total_reviews
    from tdw_ro.tdw_property_reviews_pf_v tprpv 
    join tdw_ro.tdw_property_d_v tpdv  on tprpv.property_sk = tpdv.property_sk 
    join tdw_ro.tdw_date_d_v tddv on tprpv.date_sk = tddv.date_sk
    join temp_table_redshift ttr on tpdv.src_property_id = ttr.id
    and tddv.date_dt =  TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')
    group by 1
    order by 1);
    """

    postgre_query = f"""
    select property_id, round(avg_rating::decimal(10,2),2) as avg_rating, total_reviews from (
    select ttp.id as property_id, 
    avg(cast(fr.overall_rating as float)) as avg_rating,
    cast(sum(review_count) as integer) as total_reviews
    from facility_review fr
    join temp_table_postgre ttp on fr.facility_id = ttp.gds_id
    group by 1
    order by 1);
    """

    redshift_df = fetch_data(redshift_engine, redshift_query)
    postgre_df = fetch_data(postgre_engine, postgre_query)

    redshift_grouped = redshift_df
    postgresql_grouped = postgre_df

    redshift_grouped = redshift_grouped.add_suffix('_tdw')
    postgresql_grouped = postgresql_grouped.add_suffix('_rds')

    redshift_grouped.rename(columns={'property_id_tdw':'property_id'},inplace=True)
    postgresql_grouped.rename(columns={'property_id_rds':'property_id'},inplace=True)


    if redshift_df.empty and postgre_df.empty:
        comparison_df = pd.merge(redshift_grouped, postgresql_grouped, on='property_id', how='outer', indicator=True)
        return comparison_df
    else:
        comparison_df = pd.merge(redshift_grouped, postgresql_grouped, on='property_id', how='outer', indicator=True)
        comparison_df['_merge'] = comparison_df['_merge'].map({'left_only': 'TDW', 'right_only': 'RDS', 'both': 'Both'})

        comparison_df = comparison_df.sort_values(by='property_id')
    # Determine the status
        comparison_df['status'] = comparison_df.apply(
        lambda row: 'match' if row['avg_rating_tdw'] == row['avg_rating_rds'] and row['total_reviews_tdw'] == row['total_reviews_rds'] else 'mismatch', axis=1
        )
        return comparison_df

def payment_metrics(redshift_engine,mysql_engine,tdw_date,rds_date, tdw_refunds_misc_sk):
  print("Payment metrics function")

  adjusted_end_date,first_day_of_month = setDate_Y_M_D(tdw_date)

  redshift_query = f"""      
                    with cte_payments as (
              with cte_payments1 as (Select distinct
                  p.allocated_payment_amount as ila_amount,
                  date(pdate.date_dt) as date,
                  pp.payment_method_cd as method,
                  p.payment_misc_attr_sk,
                  prop.src_property_id as property_id
                  FROM tdw_ro.tdw_payment_allocation_tf_v p
                  join tdw_ro.tdw_payment_misc_attr_d_v pp on p.payment_misc_attr_sk = pp.payment_misc_attr_sk
                --  join tdw_ro.tdw_refunds_misc_attr_d_v trmadv on p.refunds_misc_attr_sk = trmadv.refunds_misc_attr_sk
                  join tdw_ro.tdw_property_d_v prop on prop.property_sk=p.property_sk
                  left join tdw_ro.tdw_date_d_v pdate on pdate.date_sk=p.payment_date_sk
                  left join tdw_ro.tdw_date_d_v adate on adate.date_sk=p.allocation_date_sk
                  WHERE  pp.payment_method_cd not in ('credit', 'loss') and
                      pp.payment_status_cd != 0 and
                      date(pdate.date_dt) between '{first_day_of_month}' and '{adjusted_end_date}'
                      and pdate.date_dt=adate.date_dt  
                      and prop.property_sk in ({property_sk})
                      and p.refunds_misc_attr_sk = {tdw_refunds_misc_sk}
                      )
                      select ila_amount, date, method, property_id from cte_payments1 )
                            select cp.property_id, TO_CHAR(date, 'Month-YY') AS payment_month,
                            -- TO_CHAR(DATE_TRUNC('month', date), 'MM-YYYY') AS payment_month,
                  sum(case when  method = 'CASH' then ila_amount else 0 end) as pay_cash,
                  sum(case when  method = 'CHECK' then ila_amount else 0 end) as pay_check,
                  sum(case when method = 'GIFTCARD' then ila_amount else 0 end) as pay_giftcard,
                  sum(case when  method = 'ACH' then ila_amount else 0 end) as pay_ach,
                  sum(case when  method = 'CARD' then ila_amount else 0 end) as pay_card
              from cte_payments cp
              group by cp.property_id, payment_month
              order by cp.property_id, payment_month;
        """
 
  
  mysql_query =f"""
  use hummingbird;
  set @date = '{rds_date}';
  -- Payment metrics
  WITH cte_payments as(
      SELECT p.id, concat(c.first, ' ', c.last) as contact_name, u.number as unit_number, ila.amount as ila_amount, date(p.date) as date, ila.date as ila_date, ila.type as ila_type, p.method, p.property_id, p.contact_id,
          ila.invoice_line_id, i.due as invoice_due, pr.default_type as product_type, pr.name as product_name,
          pm.card_type, pm.card_end, p.number,u.type as unit_type,pr.id as product_id,
          ifnull(ppr.income_account_id,pr.income_account_id) as income_account_id,
              ifnull(pr.slug,pr.default_type) as product_slug
      FROM invoice_lines_allocation ila
          inner join invoices_payments_breakdown ipb on ipb.id = ila.invoice_payment_breakdown_id
              and ipb.refund_id is null
          inner join payments p on p.id = ipb.payment_id
          inner join invoices i on i.id = ila.invoice_id
          inner join contacts c on c.id = p.contact_id
          left join invoice_lines il on il.id = ila.invoice_line_id
          left join products pr on pr.id = il.product_id
          left join payment_methods pm on pm.id = p.payment_methods_id
          left join leases l on l.id = i.lease_id
          left join units u on u.id = l.unit_id
          left join property_products ppr on ppr.product_id = pr.id and ppr.property_id = i.property_id
      WHERE  p.credit_type = 'payment'
          and p.method not in ('credit', 'loss')
          and p.status != 0
          and date(p.date) between DATE_FORMAT(DATE_SUB(@date, INTERVAL 1 YEAR), '%Y-%m-01') and @date
          and ila.date = p.date
          and p.property_id in ({property_id}))
      SELECT p.property_id, DATE_FORMAT(p.date, '%M-%y') as payment_month,
          sum(case when p.method = 'cash' then p.ila_amount else 0 end) as month_cash,
          sum(case when p.method = 'check' then p.ila_amount else 0 end) as month_check,
          sum(case when p.method = 'ach' then p.ila_amount else 0 end) as month_ach,
          sum(case when p.method = 'card' then p.ila_amount else 0 end) as month_card,
          sum(case when p.method = 'giftcard' then p.ila_amount else 0 end) as month_gift_card
      FROM cte_payments p
      GROUP BY p.property_id, DATE_FORMAT(p.date, '%M-%y')
      ORDER BY p.property_id, DATE_FORMAT(p.date, '%M-%y');
      """

  redshift_df = fetch_data(redshift_engine, redshift_query)
  mysql_df = execute_mysql_statements(mysql_engine, mysql_query)

#   mysql_df.rename(columns={'payment_month':'date'},inplace=True)
#   mysql_df['date'] = mysql_df['date'].apply(convert_to_last_day_of_month)

  redshift_grouped = redshift_df
  mysql_grouped = mysql_df

#   redshift_grouped.rename(columns={'payment_month':'date'},inplace=True)
#   redshift_grouped['date'] = redshift_grouped['date'].apply(convert_to_last_day_by_numeric_month)

#   redshift_grouped['date'] = pd.to_datetime(redshift_grouped['date'])
#   mysql_grouped['date'] = pd.to_datetime(mysql_grouped['date'])

  redshift_grouped['payment_month'] = redshift_grouped['payment_month'].str.replace(' ', '', regex=False)  
  redshift_grouped = redshift_grouped.add_suffix('_tdw')
  mysql_grouped = mysql_grouped.add_suffix('_rds')
  redshift_grouped.rename(columns={'property_id_tdw':'property_id','payment_month_tdw':'payment_month'},inplace=True)
  mysql_grouped.rename(columns={'property_id_rds':'property_id','payment_month_rds':'payment_month'},inplace=True)
  
  if redshift_df.empty and mysql_df.empty:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','payment_month'], how='outer', indicator=True)
    return comparison_df
  else:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','payment_month'], how='outer', indicator=True)
    comparison_df['_merge'] = comparison_df['_merge'].map({'left_only': 'TDW', 'right_only': 'RDS', 'both': 'Both'})
    comparison_df['pmt'] = pd.to_datetime(comparison_df['payment_month'], format='%B-%y')
    comparison_df = comparison_df.sort_values(['property_id','pmt'],ascending=[True,False])
    comparison_df = comparison_df.drop(columns=['pmt'])

    comparison_df.fillna({'month_cash_rds':0,'month_check_rds':0,'month_ach_rds':0,'month_card_rds':0,'month_gift_card_rds':0,
                          'pay_cash_tdw':0,'pay_check_tdw':0,'pay_ach_tdw':0,'pay_card_tdw':0,'pay_giftcard_tdw':0},inplace=True)
    # Determine the status
    comparison_df['Status'] = comparison_df.apply(
        lambda row: 'match' if row['month_cash_rds'] == row['pay_cash_tdw'] and row['month_check_rds'] == row['pay_check_tdw']
        and row['month_ach_rds'] == row['pay_ach_tdw'] and row['month_card_rds'] == row['pay_card_tdw'] and 
        row['month_gift_card_rds'] == row['pay_giftcard_tdw']  else 'mismatch', axis=1
        #lambda row: 'match' if row['fee_mysql'] == row['fee_tdw'] else 'mismatch', axis=1
    )
    
    return comparison_df
  
def refunds(redshift_engine,mysql_engine,tdw_date,rds_date, tdw_refunds_misc_sk):
  print("refunds query running")
  adjusted_end_date,first_day_of_month = setDate_Y_M_D(tdw_date)

  redshift_query = f"""      
                  SELECT distinct
                      prop.src_property_id AS property_id,
                      TO_CHAR(adate.date_dt, 'Month-YY') AS refund_month,
                      -- TO_CHAR(DATE_TRUNC('month', adate.date_dt), 'MM-YYYY') AS refund_month,
                       SUM(p.ila_amount) AS total_amount,
                      SUM(CASE WHEN r.refund_type_cd = 'nsf' THEN -p.ila_amount  ELSE 0 END) AS NSF,
                      SUM(CASE WHEN r.refund_type_cd = 'ach' THEN -p.ila_amount  ELSE 0 END) AS ACH,
                      SUM(CASE WHEN r.refund_type_cd = 'chargeback' THEN -p.ila_amount  ELSE 0 END) AS Chargeback,
                      SUM(CASE WHEN r.refund_type_cd NOT IN ('nsf', 'ach', 'chargeback') THEN -p.ila_amount  ELSE 0 END) AS reversals
                  FROM
                      tdw_ro.tdw_refunds_tf_v p
                  LEFT JOIN
                      tdw_ro.tdw_refunds_misc_attr_d_v  r ON p.refunds_misc_attr_sk  = r.refunds_misc_attr_sk
                  JOIN
                      tdw_ro.tdw_property_d_v prop ON prop.property_sk = p.property_sk
                  LEFT JOIN
                      tdw_ro.tdw_date_d_v adate ON adate.date_sk = p.ila_date_sk  
                  WHERE
                  p.credit_type  = 'payment'  
                  and p.method not in ('credit', 'loss')
                  and   p.status != 0
                  and    adate.date_dt BETWEEN '{first_day_of_month}' AND '{adjusted_end_date}'
                      AND prop.property_sk  in ({property_sk})
                      and r.refunds_misc_attr_sk <> {tdw_refunds_misc_sk}
                  GROUP BY
                      prop.src_property_id,
                      TO_CHAR(adate.date_dt, 'Month-YY')
                      order by prop.src_property_id,
                      TO_CHAR(adate.date_dt, 'Month-YY');
        """
  
  mysql_query =f"""
  use hummingbird;
  set @date = '{rds_date}';
  -- refunds
  WITH cte_payments as (
 SELECT p.id, r.id as refund_id, ila.amount as ila_amount, ila.type as ila_type, p.method, ila.date as ila_date, p.property_id, r.refund_to as contact_id, 
                ila.invoice_line_id, i.due as invoice_due, pr.default_type as product_type, pr.name as product_name, r.type as refund_type,
                pm.card_type, pm.card_end, p.number, p.date as payment_date,u.type as unit_type,pr.id as product_id,
                ifnull(ppr.income_account_id,pr.income_account_id) as income_account_id,
				        ifnull(pr.slug,pr.default_type) as product_slug
            FROM invoice_lines_allocation ila
                inner join invoices_payments_breakdown ipb on ipb.id = ila.invoice_payment_breakdown_id
                inner join payments p on p.id = ipb.payment_id
                inner join invoices i on i.id = ila.invoice_id
                inner join refunds r on r.id = ipb.refund_id
                left join invoice_lines il on il.id = ila.invoice_line_id
                left join products pr on pr.id = il.product_id
                left join payment_methods pm on pm.id = p.payment_methods_id
                left join leases l on l.id = i.lease_id
                left join units u on u.id = l.unit_id
                left join property_products ppr on ppr.product_id = pr.id and ppr.property_id = i.property_id 
            WHERE  p.credit_type = 'payment'
                and p.method not in ('credit', 'loss')
                and p.status != 0
                and ila.date between '{first_day_of_month}' AND '{adjusted_end_date}'
                and p.property_id in ({property_id})

)
            SELECT p.property_id , p.contact_id, DATE_FORMAT(ila_date, '%M-%y') as refund_month,
            -- DATE_FORMAT(ila_date, '%M-%y') as refund_month, 
            SUM(ila_amount) AS total_amount,
              -1 * sum(case when p.refund_type = 'nsf' then ila_amount else 0 end ) as nsf,
              -1 * sum(case when p.refund_type = 'chargeback' then ila_amount else 0 end ) as chargebacks,
              -1 * sum(case when p.refund_type = 'ach' then ila_amount else 0 end ) as ach,
              -1 * sum(case when p.refund_type not in ('nsf', 'chargeback', 'ach') then ila_amount else 0 end ) as reversals
            FROM cte_payments p      
            GROUP BY p.property_id, DATE_FORMAT(ila_date, '%M-%y')
            ORDER BY p.property_id, DATE_FORMAT(ila_date, '%M-%y');

      """


  redshift_df = fetch_data(redshift_engine, redshift_query)
  mysql_df = execute_mysql_statements(mysql_engine, mysql_query)
 
#   print(redshift_df)
#   print(mysql_df)
#   mysql_df.rename(columns={'refund_month':'date'},inplace=True)
#   mysql_df['date'] = mysql_df['date'].apply(convert_to_last_day_of_month)
#   redshift_df.rename(columns={'refund_month':'date'},inplace=True)
#   redshift_df['date'] = redshift_df['date'].apply(convert_to_last_day_by_numeric_month)

  redshift_grouped = redshift_df
  mysql_grouped = mysql_df

  redshift_grouped['refund_month'] = redshift_grouped['refund_month'].str.replace(' ', '', regex=False)  

#   redshift_grouped['date'] = pd.to_datetime(redshift_grouped['date'])
#   mysql_grouped['date'] = pd.to_datetime(mysql_grouped['date'])


  redshift_grouped = redshift_grouped.add_suffix('_tdw')
  mysql_grouped = mysql_grouped.add_suffix('_rds')
  redshift_grouped.rename(columns={'property_id_tdw':'property_id','refund_month_tdw':'refund_month'},inplace=True)
  mysql_grouped.rename(columns={'property_id_rds':'property_id','refund_month_rds':'refund_month'},inplace=True)
  
  if redshift_df.empty and mysql_df.empty:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','refund_month'], how='outer', indicator=True)
    return comparison_df
  else:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','refund_month'], how='outer', indicator=True)
    comparison_df['_merge'] = comparison_df['_merge'].map({'left_only': 'TDW', 'right_only': 'RDS', 'both': 'Both'})
    comparison_df['rfd'] = pd.to_datetime(comparison_df['refund_month'], format='%B-%y')
    comparison_df = comparison_df.sort_values(['property_id','rfd'],ascending=[True,False])
    comparison_df = comparison_df.drop(columns=['rfd'])

    # Determine the status
    comparison_df['Status'] = comparison_df.apply(
        lambda row: 'match' if row['total_amount_rds'] == row['total_amount_tdw'] and row['nsf_rds'] == row['nsf_tdw'] and row['chargebacks_rds'] == row['chargeback_tdw'] and
        row['ach_rds'] == row['ach_tdw'] and row['reversals_rds'] == row['reversals_tdw']  else 'mismatch', axis=1
    )
    return comparison_df

def credits_adjustments(redshift_engine,mysql_engine,tdw_date,rds_date):
  print("creadits and adjustment query running")
  adjusted_end_date,first_day_of_month = setDate_Y_M_D(tdw_date)

  redshift_query = f"""       
      with cte_credits_and_adjustments as(
    select distinct property_id,adjustment_credit,date,adjustment_credit_month,sum(payment_allocation_amount) as amount from (
    select distinct p.src_payment_allocation_id,
    p.src_payment_breakdown_id as payment_breakdown_id,
    prop.src_property_id as property_id,
    p.allocated_payment_amount as payment_allocation_amount ,
    pp.adjustment_reason_cd as adjustment_credit,
    pd.date_dt as date,
    TO_CHAR(pd.date_dt, 'Month-YY') as adjustment_credit_month
    from
    tdw_ro.tdw_payment_allocation_tf_v p
    join tdw_ro.tdw_payment_misc_attr_d_v pp on pp.payment_misc_attr_sk=p.payment_misc_attr_sk
    left join tdw_ro.tdw_date_d_v pd on pd.date_sk=p.payment_date_sk
    left join tdw_ro.tdw_date_d_v pbd on pbd.date_sk=p.payment_breakdown_date_sk
    left join tdw_ro.tdw_property_d_v prop on prop.property_Sk=p.property_sk
    where
    pp.credit_type_desc = 'adjustment'
    and
    pd.date_dt between'{first_day_of_month}' and '{adjusted_end_date}'
    and
    pbd.date_dt between '{first_day_of_month}' and '{adjusted_end_date}'
    and prop.property_sk in ({property_sk})
    )t
    group by property_id,adjustment_credit,date,adjustment_credit_month
    union all
    select distinct property_id,adjustment_credit,date,adjustment_credit_month,sum(payment_allocation_amount) as amount from (
    select distinct p.src_payment_allocation_id,
    p.src_payment_breakdown_id as payment_breakdown_id,
    prop.src_property_id as property_id,
    p.allocated_payment_amount as payment_allocation_amount,
    pp.credit_type_desc as adjustment_credit,
    pd.date_dt as date,
    TO_CHAR(pd.date_dt, 'Month-YY') as adjustment_credit_month
    from
    tdw_ro.tdw_payment_allocation_tf_v p
    join tdw_ro.tdw_payment_misc_attr_d_v pp on pp.payment_misc_attr_sk=p.payment_misc_attr_sk
    join tdw_ro.tdw_date_d_v pd on pd.date_sk=p.payment_date_sk
    join tdw_ro.tdw_date_d_v pbd on pbd.date_sk=p.payment_breakdown_date_sk
    join tdw_ro.tdw_property_d_v prop on prop.property_Sk=p.property_sk
    where
    pp.credit_type_desc = 'credit'
    and
    pd.date_dt between '{first_day_of_month}' and '{adjusted_end_date}'
    and
    pbd.date_dt between '{first_day_of_month}' and '{adjusted_end_date}'
    and prop.property_sk in ({property_sk})
    )t1
    group by property_id,adjustment_credit,date,adjustment_credit_month)
    select distinct cca.property_id,
        adjustment_credit_month,
        sum(case when cca.adjustment_credit = 'move_out' then cca.amount else 0 end) as move_out,
        sum(case when cca.adjustment_credit = 'auction' then cca.amount else 0 end) as auction,
        sum(case when cca.adjustment_credit = 'transfer' then cca.amount else 0 end) as transfer,
        sum(case when cca.adjustment_credit = 'cleaning_deposit' then cca.amount else 0 end) as cleaning_deposit,
        sum(case when cca.adjustment_credit = 'credit' then cca.amount else 0 end) as credits,
        sum(case when cca.adjustment_credit = 'security_deposit' then cca.amount else 0 end) as security_deposit,
        (sum(case when cca.adjustment_credit = 'move_out' then cca.amount else 0 end) +
        sum(case when cca.adjustment_credit = 'auction' then cca.amount else 0 end) +
        sum(case when cca.adjustment_credit = 'transfer' then cca.amount else 0 end) +
        sum(case when cca.adjustment_credit = 'cleaning_deposit' then cca.amount else 0 end) +
        sum(case when cca.adjustment_credit = 'credit' then cca.amount else 0 end) +
        sum(case when cca.adjustment_credit = 'security_deposit' then cca.amount else 0 end) ) total
        from cte_credits_and_adjustments cca
        group by cca.property_id, adjustment_credit_month;
        """
  
  mysql_query =f"""
    use hummingbird;
    set @date = '{rds_date}';
    
    with cte_credits_and_adjustments as( select
    p.property_id,
    p.sub_method as adjustment_credit,
    sum(ipb.amount) as amount,
    date(p.date) as date
    from payments p
    inner join invoices_payments_breakdown ipb on ipb.payment_id = p.id and date(ipb.date) between DATE_FORMAT(DATE_SUB(@date, INTERVAL 1 YEAR), '%Y-%m-01') and @date
    where p.credit_type = 'adjustment' and
    p.property_id in ({property_id}) and
    p.date between DATE_FORMAT(DATE_SUB(@date, INTERVAL 1 YEAR), '%Y-%m-01') and @date
    group by p.id

    UNION ALL
    select
    p.property_id,
    p.credit_type as adjustment_credit,
    sum(ipb.amount) as amount,
    date(p.date)
    from payments p 
    inner join invoices_payments_breakdown ipb on ipb.payment_id = p.id and date(ipb.date) between DATE_FORMAT(DATE_SUB(@date, INTERVAL 1 YEAR), '%Y-%m-01') and @date
    where p.credit_type = 'credit' and
    p.property_id in ({property_id}) and
    date(p.date) between DATE_FORMAT(DATE_SUB(@date, INTERVAL 1 YEAR), '%Y-%m-01') and @date
    group by p.id)
    
    select cca.property_id,
    DATE_FORMAT(cca.date, '%M-%y') as adjustment_credit_month,
    sum(case when cca.adjustment_credit = 'move_out' then cca.amount else 0 end) as move_out,
    sum(case when cca.adjustment_credit = 'auction' then cca.amount else 0 end) as auction,
    sum(case when cca.adjustment_credit = 'transfer' then cca.amount else 0 end) as transfer,
    sum(case when cca.adjustment_credit = 'cleaning_deposit' then cca.amount else 0 end) as cleaning_deposit,
    sum(case when cca.adjustment_credit = 'credit' then cca.amount else 0 end) as credits,
    sum(case when cca.adjustment_credit = 'security_deposit' then cca.amount else 0 end) as security_deposit
    from cte_credits_and_adjustments cca
    group by cca.property_id, adjustment_credit_month
    order by cca.date desc;
     """

  redshift_df = fetch_data(redshift_engine, redshift_query)
  mysql_df = execute_mysql_statements_credits(mysql_engine, mysql_query)

  redshift_grouped = redshift_df
  mysql_grouped = mysql_df

  redshift_grouped['adjustment_credit_month'] = redshift_grouped['adjustment_credit_month'].str.replace(' ', '', regex=False)
  redshift_grouped = redshift_grouped.add_suffix('_tdw')
  mysql_grouped = mysql_grouped.add_suffix('_rds')
  redshift_grouped.rename(columns={'property_id_tdw':'property_id','adjustment_credit_month_tdw':'adjustment_credit_month'},inplace=True)
  mysql_grouped.rename(columns={'property_id_rds':'property_id','adjustment_credit_month_rds':'adjustment_credit_month'},inplace=True)
  redshift_grouped = redshift_grouped[['property_id','adjustment_credit_month','move_out_tdw','auction_tdw','transfer_tdw','cleaning_deposit_tdw','credits_tdw','security_deposit_tdw']]
  mysql_grouped = mysql_grouped[['property_id','adjustment_credit_month','move_out_rds','auction_rds','transfer_rds','cleaning_deposit_rds','credits_rds','security_deposit_rds']]
  
  if redshift_df.empty and mysql_df.empty:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','adjustment_credit_month'], how='outer', indicator=True)
    return comparison_df
  else:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','adjustment_credit_month'], how='outer', indicator=True)
    comparison_df['_merge'] = comparison_df['_merge'].map({'left_only': 'TDW', 'right_only': 'RDS', 'both': 'Both'})
    comparison_df['acm'] = pd.to_datetime(comparison_df['adjustment_credit_month'], format='%B-%y')
    comparison_df = comparison_df.sort_values(['property_id','acm'],ascending=[True,False])
    comparison_df = comparison_df.drop(columns='acm')

    comparison_df['Status'] = comparison_df.apply(
        lambda row: 'match' if row['move_out_tdw'] == row['move_out_rds'] and row['auction_tdw'] == row['auction_rds'] and 
        row['transfer_tdw'] == row['transfer_rds']   and row['cleaning_deposit_tdw'] == row['cleaning_deposit_rds'] and 
        row['credits_tdw'] == row['credits_rds'] and row['security_deposit_tdw'] == row['security_deposit_rds'] else 'mismatch', axis=1
    )
    return comparison_df
  
def Move_in(redshift_engine,mysql_engine,tdw_date,rds_date):
  print("move in query running")

  adjusted_end_date,first_day_of_month = setDate_M_D_Y(tdw_date)

  redshift_query =f"""       
      Select tpdv.src_property_id as property_id,
      TO_CHAR(d.date_dt, 'Month-YY') AS move_in_month,
      -- d.calendar_month , 
      count(move_in_count) as move_in_count
      From     tdw_ro.tdw_leases_af_v lt
      Join    tdw_ro.tdw_date_d_v d on lt.move_in_date_sk = d.date_sk
      --Join    tdw_ro.tdw_date_d_v da on lt.move_in_date_sk = da.date_sk
      join tdw_ro.tdw_property_d_v tpdv  on tpdv.property_sk = lt.property_sk 
      Where     tpdv.property_sk in ({property_sk}) and 
      d.date_dt  between '{first_day_of_month}' and '{adjusted_end_date}'
          and move_in_count = 1    and transfer_in_count = 0 and tenant_sk <>76376
          group by tpdv.src_property_id,TO_CHAR(d.date_dt, 'Month-YY')
          order by tpdv.src_property_id,TO_CHAR(d.date_dt, 'Month-YY'); 
      """
  
  mysql_query =f"""
  use hummingbird;
  set @date = '{rds_date}';
  
  WITH cte_move_ins as ( 
      select p.id as property_id, u.number as unit_number, u.type as unit_type, u.id as unit_id, c.id as contact_id, l.id as lease_id, comp.name as company_name, u.label as unit_size, uc.name as category_name, concat(c.first,' ',c.last) as name, l.start_date as move_in_date, 
          ((SELECT IFNULL(SUM(price),0) FROM services where product_id in (select id from products where default_type = 'rent') and lease_id = l.id  AND status = 1 and start_date <= l.start_date and (end_date is null or end_date > l.start_date) )) as rent,
          ((SELECT IFNULL(SUM(price),0) FROM services where product_id in (select id from products where default_type = 'insurance') and lease_id = l.id  AND status = 1 and start_date <= l.start_date and (end_date is null or end_date > l.start_date) )) as insurance_premium,
          (SELECT IFNULL(SUM(total_discounts),0) from invoices where lease_id = l.id and status = 1 and DATE(period_start) = l.start_date) as promotion_amount,
          (SELECT GROUP_CONCAT(name SEPARATOR ', ') from promotions where id in (SELECT promotion_id from discounts where lease_id = l.id and start = l.start_date)) as promotion_names,
          ((SELECT IFNULL( (SELECT price from unit_price_changes where DATE(created) <= l.start_date and unit_id = l.unit_id order by id desc limit 1),(SELECT upc.set_rate from unit_price_changes upc where DATE(upc.created) <= CURRENT_DATE() and upc.unit_id = l.unit_id order by upc.id DESC limit 1)) as price )) as space_rate,
          (((SELECT IFNULL( (SELECT price from unit_price_changes where DATE(created) <= l.start_date and unit_id = l.unit_id order by id desc limit 1),(SELECT upc.set_rate from unit_price_changes upc where DATE(upc.created) <= CURRENT_DATE() and upc.unit_id = l.unit_id order by upc.id DESC limit 1)) as price )) - ((SELECT IFNULL(SUM(price),0) FROM services where product_id in (select id from products where default_type = 'rent') and lease_id = l.id  AND status = 1 and start_date <= l.start_date and (end_date is null or end_date > l.start_date) ))) as variance,
          (SELECT IFNULL(DATEDIFF(l.start_date, (select MAX(end_date) from leases WHERE end_date <= l.start_date and  status = 1 and unit_id = l.unit_id)),0) ) as days_vacant,
          ( (SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap join units un on un.property_id=ap.property_id where ap.amenity_name = 'width' and ap.property_type = 'storage' and un.id = l.unit_id) and unit_id = l.unit_id) *  (SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap join units un on un.property_id=ap.property_id where ap.amenity_name = 'length' and ap.property_type = 'storage' and un.id = l.unit_id) and unit_id = l.unit_id)) as unit_area      
      from  leases as l
          inner join units u on l.unit_id = u.id        
          inner join properties p on u.property_id = p.id
          inner join contact_leases cl on cl.lease_id = l.id
          inner join contacts c on c.id = cl.contact_id
          inner join companies comp on c.company_id = comp.id
          left join unit_categories uc on u.category_id = uc.id
      where l.start_date >= DATE_FORMAT(DATE_SUB(@date, INTERVAL 1 YEAR), '%Y-%m-01')
          and l.start_date <= @date
          and u.property_id in ({property_id})
          and l.id not in (select to_lease_id from transfers where to_lease_id = l.id)
          and l.status = 1
          and cl.primary = 1
      )
      SELECT property_id,DATE_FORMAT(cmi.move_in_date, '%M-%y') as move_in_month, count(*) as move_in_count
      FROM cte_move_ins cmi
      GROUP BY property_id,DATE_FORMAT(cmi.move_in_date, '%M-%y')
      ORDER BY property_id,DATE_FORMAT(cmi.move_in_date, '%M-%y');
     """

  redshift_df = fetch_data(redshift_engine, redshift_query)
  mysql_df = execute_mysql_statements(mysql_engine, mysql_query)

#   mysql_df.rename(columns={'move_in_month':'date'},inplace=True)
#   mysql_df['date'] = mysql_df['date'].apply(convert_to_last_day)

#   redshift_df.rename(columns={'calendar_month':'date'},inplace=True)
#   redshift_df['date'] = redshift_df['date'].apply(convert_to_last_day_by_numeric_month)

  redshift_grouped = redshift_df
  mysql_grouped = mysql_df
  
  redshift_grouped['move_in_month'] = redshift_grouped['move_in_month'].str.replace(' ', '', regex=False)  

#   redshift_grouped['date'] = pd.to_datetime(redshift_grouped['date'])
#   mysql_grouped['date'] = pd.to_datetime(mysql_grouped['date'])

  redshift_grouped = redshift_grouped.add_suffix('_tdw')
  mysql_grouped = mysql_grouped.add_suffix('_rds')
  redshift_grouped.rename(columns={'property_id_tdw':'property_id','move_in_month_tdw':'move_in_month'},inplace=True)
  mysql_grouped.rename(columns={'property_id_rds':'property_id','move_in_month_rds':'move_in_month'},inplace=True)
  
  if redshift_df.empty and mysql_df.empty:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','move_in_month'], how='outer', indicator=True)
    return comparison_df
  else:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','move_in_month'], how='outer', indicator=True)
    comparison_df['_merge'] = comparison_df['_merge'].map({'left_only': 'TDW', 'right_only': 'RDS', 'both': 'Both'})
    comparison_df['mim'] = pd.to_datetime(comparison_df['move_in_month'],format= '%B-%y')
    comparison_df = comparison_df.sort_values(['property_id','mim'],ascending=[True,False])
    comparison_df = comparison_df.drop(columns=['mim'])

    comparison_df['Status'] = comparison_df.apply(
        lambda row: 'match' if row['move_in_count_rds'] == row['move_in_count_tdw'] else 'mismatch', axis=1
    )
    return comparison_df
  
def Move_out(redshift_engine,mysql_engine,tdw_date,rds_date):
  print("move out query running")  

  adjusted_end_date,first_day_of_month = setDate_M_D_Y(tdw_date)

  redshift_query =f"""       
    Select tpdv.src_property_id as property_id,
    TO_CHAR(d.date_dt, 'Month-YY') AS move_out_month,
    -- d.calendar_month , 
    count(*) as move_out_count
    From tdw_ro.tdw_leases_af_v lt
    Join tdw_ro.tdw_date_d_v d on lt.move_out_date_sk = d.date_sk
    join tdw_ro.tdw_property_d_v tpdv  on tpdv.property_sk = lt.property_sk
    Where     tpdv.property_sk  in ({property_sk}) 
    and d.date_dt  between '{first_day_of_month}' and '{adjusted_end_date}'
    and move_out_count = 1    and transfer_out_count = 0 and tenant_sk <>76376
    group by tpdv.src_property_id,TO_CHAR(d.date_dt, 'Month-YY')
    order by tpdv.src_property_id,TO_CHAR(d.date_dt, 'Month-YY');
      """
  
  mysql_query = f"""
use hummingbird;    
set @date = '{rds_date}';
WITH cte_move_outs as (
         select p.id as property_id, u.number as unit_number, u.type as unit_type, u.id as unit_id, c.id as contact_id,l.id as lease_id, comp.name as company_name, u.label as unit_size, uc.name as category_name, concat(c.first,' ',c.last) as name, l.end_date as move_out_date,
         ((SELECT IFNULL(SUM(price),0) FROM services where product_id in (select id from products where default_type = 'rent') and lease_id = l.id  AND status = 1 and start_date <= l.start_date and (end_date is null or end_date > l.start_date) )) as rent,
         ((SELECT IFNULL(SUM(price),0) FROM services where product_id in (select id from products where default_type = 'insurance') and lease_id = l.id  AND status = 1 and start_date <= l.start_date and (end_date is null or end_date > l.start_date) )) as insurance_premium,
         ((SELECT IFNULL( (SELECT price from unit_price_changes where DATE(created) <= l.start_date and unit_id = l.unit_id order by id desc limit 1),(SELECT upc.set_rate from unit_price_changes upc where DATE(upc.created) <= CURRENT_DATE() and upc.unit_id = l.unit_id order by upc.id DESC limit 1)) as price )) as space_rate,
         (((SELECT IFNULL( (SELECT price from unit_price_changes where DATE(created) <= l.start_date and unit_id = l.unit_id order by id desc limit 1),(SELECT upc.set_rate from unit_price_changes upc where DATE(upc.created) <= CURRENT_DATE() and upc.unit_id = l.unit_id order by upc.id DESC limit 1)) as price )) - ((SELECT IFNULL(SUM(price),0) FROM services where product_id in (select id from products where default_type = 'rent') and lease_id = l.id  AND status = 1 and start_date <= l.start_date and (end_date is null or end_date > l.start_date) ))) as variance,
         (SELECT IFNULL(DATEDIFF(l.end_date, l.start_date),0)) as days_in_space,
         (SELECT IF((SELECT id from lease_auctions where lease_id = l.id and deleted_at IS NULL having max(id)) IS NOT NULL, 'Yes', 'No')) as auction,
         ( (SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap join units un on un.property_id=ap.property_id where ap.amenity_name = 'width' and ap.property_type = 'storage' and un.id = l.unit_id) and unit_id = l.unit_id) *  (SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap join units un on un.property_id=ap.property_id where ap.amenity_name = 'length' and ap.property_type = 'storage' and un.id = l.unit_id) and unit_id = l.unit_id)) as unit_area
       from  leases as l
         inner join units u on l.unit_id = u.id
         inner join properties p on u.property_id = p.id
         inner join contact_leases cl on cl.lease_id = l.id
         inner join contacts c on c.id = cl.contact_id
         inner join companies comp on c.company_id = comp.id
         left join unit_categories uc on u.category_id = uc.id
       where l.end_date >= DATE_FORMAT(DATE_SUB(@date, INTERVAL 1 YEAR), '%Y-%m-01')
         and l.end_date <= @date
         and u.property_id in ({property_id})
         and l.id not in (select from_lease_id from transfers where from_lease_id = l.id)
         and l.status = 1
         and cl.primary = 1
     )
       SELECT property_id,DATE_FORMAT(cmo.move_out_date, '%M-%y') as move_out_month, count(*) as move_out_count
       FROM cte_move_outs cmo
       GROUP BY property_id,DATE_FORMAT(cmo.move_out_date, '%M-%y')
       ORDER BY property_id,DATE_FORMAT(cmo.move_out_date, '%M-%y')
     """

  redshift_df = fetch_data(redshift_engine, redshift_query)
  mysql_df = execute_mysql_statements(mysql_engine, mysql_query)


#   mysql_df.rename(columns={'move_out_month':'date'},inplace=True)
#   mysql_df['date'] = mysql_df['date'].apply(convert_to_last_day)

#   redshift_df.rename(columns={'calendar_month':'date'},inplace=True)
#   redshift_df['date'] = redshift_df['date'].apply(convert_to_last_day_by_numeric_month)

  redshift_grouped = redshift_df
  mysql_grouped = mysql_df

  redshift_grouped['move_out_month'] = redshift_grouped['move_out_month'].str.replace(' ', '', regex=False)  
#   redshift_grouped['date'] = pd.to_datetime(redshift_grouped['date'])
#   mysql_grouped['date'] = pd.to_datetime(mysql_grouped['date'])

  redshift_grouped = redshift_grouped.add_suffix('_tdw')
  mysql_grouped = mysql_grouped.add_suffix('_rds')
  redshift_grouped.rename(columns={'property_id_tdw':'property_id','move_out_month_tdw':'move_out_month'},inplace=True)
  mysql_grouped.rename(columns={'property_id_rds':'property_id','move_out_month_rds':'move_out_month'},inplace=True)
  
  if redshift_df.empty and mysql_df.empty:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','move_out_month'], how='outer', indicator=True)
    return comparison_df
  else:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','move_out_month'], how='outer', indicator=True)
    comparison_df['_merge'] = comparison_df['_merge'].map({'left_only': 'TDW', 'right_only': 'RDS', 'both': 'Both'})
    comparison_df['mom'] = pd.to_datetime(comparison_df['move_out_month'],format= '%B-%y')
    comparison_df = comparison_df.sort_values(['property_id','mom'],ascending=[True,False])
    comparison_df = comparison_df.drop(columns=['mom'])

    comparison_df['Status'] = comparison_df.apply(
        lambda row: 'match' if row['move_out_count_rds'] == row['move_out_count_tdw'] else 'mismatch', axis=1
    )
    return comparison_df
  
def Delinquency(redshift_engine,mysql_engine,tdw_date,rds_date):
  print("delinquency query running")
  adjusted_end_date = pd.to_datetime(tdw_date)
  last_day_of_month = adjusted_end_date + pd.offsets.MonthEnd(0)
  dates = [adjusted_end_date]
  for i in range(1, 13):
      date = adjusted_end_date - pd.DateOffset(months=i)
      last_day_of_month = date + pd.offsets.MonthEnd(0)
      dates.append(last_day_of_month)
      
  date_strs = [date.strftime('%Y-%m-%d') for date in dates]
  date_condition = "d1.date_dt IN (" + ", ".join([f"'{date}'" for date in date_strs]) + ")"

  redshift_query =f"""       
   with del as (
  SELECT
        f.space_sk,
        f.property_sk,
        t.src_property_id as property_id,
        f.invoice_amount,
        d1.date_dt,
        DATEDIFF(day, d2.date_dt, d1.date_dt) AS days_unpaid
    FROM
        tdw_ro.tdw_delinquency_pf_v f
    join tdw_ro.tdw_property_d_v t on f.property_sk = t.property_sk
    JOIN tdw_ro.tdw_date_d_v d1 ON f.date_sk = d1.date_sk
    JOIN tdw_ro.tdw_date_d_v d2 ON f.invoice_due_date_sk = d2.date_sk
    WHERE
        {date_condition}
        AND f.property_sk in ({property_sk})
    )    
        SELECT    property_id,    to_char(date_dt,'Month-YY') as calendar_month,
    SUM(CASE WHEN days_unpaid BETWEEN 0 AND 10 THEN invoice_amount ELSE 0 END) AS amount_0_10,
    SUM(CASE WHEN days_unpaid BETWEEN 11 AND 30 THEN invoice_amount ELSE 0 END) AS amount_11_30,
    SUM(CASE WHEN days_unpaid BETWEEN 31 AND 60 THEN invoice_amount ELSE 0 END) AS amount_31_60,
    SUM(CASE WHEN days_unpaid BETWEEN 61 AND 90 THEN invoice_amount ELSE 0 END) AS amount_61_90,
    SUM(CASE WHEN days_unpaid BETWEEN 91 AND 120 THEN invoice_amount ELSE 0 END) AS amount_91_120,
    SUM(CASE WHEN days_unpaid BETWEEN 121 AND 180 THEN invoice_amount ELSE 0 END) AS amount_121_180,
    SUM(CASE WHEN days_unpaid BETWEEN 181 AND 360 THEN invoice_amount ELSE 0 END) AS amount_181_360,
    SUM(CASE WHEN days_unpaid > 360 THEN invoice_amount ELSE 0 END) AS amount_361
FROM    del
GROUP by property_id,to_char(date_dt,'Month-YY')
order by property_id,to_char(date_dt,'Month-YY');

      """
  
  mysql_query =f"""
use hummingbird; 
set @date = '{rds_date}';    
    
-- $

 with cte_invoices as ( 
             select u.property_id, t.date as report_date, i.lease_id, i.id as invoice_id, i.number as invoice_number,
                 concat(c.first,' ',c.last) as name, u.number as unit_number, l.end_date as move_out_date,
                 i.date as invoice_date, i.due as invoice_due, timestampdiff(day, i.due, t.date) as days_unpaid,
                 (ifnull(i.subtotal, 0) + ifnull(i.total_tax , 0) - ifnull(i.total_discounts ,0)) as total_amount,
                 sum(ifnull(ipb.amount,0)) as total_paid,
                 l.start_date as move_in_date
             from invoices i
                 join leases l on l.id = i.lease_id and l.status = 1
            join (SELECT @date as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 1 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 2 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 3 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 4 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 5 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 6 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 7 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 8 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 9 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 10 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 11 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 12 MONTH)) as date) t
                     on i.due <= t.date and (i.void_date is null or convert(date_format(i.void_date ,'%Y-%m-%d'),DATE) > t.date)
                         and (l.end_date is null or l.end_date > t.date)
                 join units u on u.id = l.unit_id
                 left outer join invoices_payments_breakdown ipb on ipb.invoice_id = i.id and ipb.date <= t.date
                 join contact_leases cl on cl.lease_id = i.lease_id and cl.primary = 1
                 join contacts c on c.id = cl.contact_id
             where u.property_id in ({property_id})
             group by u.property_id, i.lease_id, i.id, days_unpaid, total_amount, t.date
             having ifnull(total_amount, 0) - ifnull(total_paid, 0) > 0
         )
             SELECT property_id, DATE_FORMAT(report_date, '%M-%y') as calendar_month,
                 sum(case when days_unpaid between 0 and 10 then total_amount - total_paid else 0 end) as delinquent_amount_10,
                 sum(case when days_unpaid between 11 and 30 then total_amount - total_paid else 0 end) as delinquent_amount_30,
                 sum(case when days_unpaid between 31 and 60 then total_amount - total_paid else 0 end) as delinquent_amount_60,
                 sum(case when days_unpaid between 61 and 90 then total_amount - total_paid else 0 end) as delinquent_amount_90,
                 sum(case when days_unpaid between 91 and 120 then total_amount - total_paid else 0 end) as delinquent_amount_120,
                 sum(case when days_unpaid between 121 and 180 then total_amount - total_paid else 0 end) as delinquent_amount_180,
                 sum(case when days_unpaid between 181 and 360 then total_amount - total_paid else 0 end) as delinquent_amount_360,
                 sum(case when days_unpaid > 360 then total_amount - total_paid else 0 end) as delinquent_amount_gtr_360
             from cte_invoices
             group by property_id, DATE_FORMAT(report_date, '%M-%y')
             order by property_id, DATE_FORMAT(report_date, '%M-%y');
     """

  redshift_df = fetch_data(redshift_engine, redshift_query)
  mysql_df = execute_mysql_statements(mysql_engine, mysql_query)

#   mysql_df['date'] = mysql_df['month'].apply(convert_to_last_day)

  redshift_grouped = redshift_df
  mysql_grouped = mysql_df
  redshift_grouped['calendar_month'] = redshift_grouped['calendar_month'].str.replace(' ', '', regex=False)  

#   redshift_grouped.rename(columns={'date_dt':'date'},inplace=True)
#   redshift_grouped['date'] = pd.to_datetime(redshift_grouped['date'])
#   mysql_grouped['date'] = pd.to_datetime(mysql_grouped['date'])
  

  redshift_grouped = redshift_grouped.add_suffix('_tdw')
  mysql_grouped = mysql_grouped.add_suffix('_rds')
  redshift_grouped.rename(columns={'property_id_tdw':'property_id','calendar_month_tdw':'calendar_month'},inplace=True)
  mysql_grouped.rename(columns={'property_id_rds':'property_id','calendar_month_rds':'calendar_month'},inplace=True)
  
  if redshift_df.empty and mysql_df.empty:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','calendar_month'], how='outer', indicator=True)
    return comparison_df
  else:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','calendar_month'], how='outer', indicator=True)
    comparison_df['_merge'] = comparison_df['_merge'].map({'left_only': 'TDW', 'right_only': 'RDS', 'both': 'Both'})
    comparison_df['cm'] = pd.to_datetime(comparison_df['calendar_month'],format= '%B-%y')
    comparison_df = comparison_df.sort_values(['property_id','cm'],ascending=[True,False])
    comparison_df = comparison_df.drop(columns=['cm'])

    comparison_df['Status'] = comparison_df.apply(
        lambda row: 'match' if   row['delinquent_amount_10_rds'] == row['amount_0_10_tdw'] and row['delinquent_amount_30_rds'] == row['amount_11_30_tdw'] and row['delinquent_amount_60_rds'] == row['amount_31_60_tdw'] and row['delinquent_amount_90_rds'] == row['amount_61_90_tdw'] and row['delinquent_amount_120_rds'] == row['amount_91_120_tdw'] and row['delinquent_amount_180_rds'] == row['amount_121_180_tdw'] and row['delinquent_amount_360_rds'] == row['amount_181_360_tdw'] and row['delinquent_amount_gtr_360_rds'] == row['amount_361_tdw'] else 'mismatch', axis=1
    )
    return comparison_df
  

def reservation(redshift_engine,mysql_engine,tdw_date,rds_date):
  print("reservation query running")  

  adjusted_end_date,first_day_of_month = setDate_M_D_Y(tdw_date)

  redshift_query =f"""       
    select tpdv2.src_property_id as property_id,
    TO_CHAR(tddv.date_dt, 'Month-YY') as month,
    -- TO_CHAR(DATE_TRUNC('month', tddv.date_dt), 'MM-YYYY') as month, 
    sum(reservation_count) as reservation_count
    from tdw_ro.tdw_reservations_af_v s
    join tdw_ro.tdw_property_d_v tpdv2 on tpdv2.property_sk = s.property_sk
    join tdw_ro.tdw_tenant_d_v t on t.tenant_sk = s.tenant_sk
    join tdw_ro.tdw_date_d_v tddv   on s.start_date_sk = tddv.date_sk
    --left join tdw_ro.tdw_date_d_v tddv2   on s.expires_date_sk = tddv.date_sk
    where tddv.date_dt  between '{first_day_of_month}' and '{adjusted_end_date}'
    and tpdv2.property_sk  in ({property_sk})
    GROUP BY tpdv2.src_property_id,TO_CHAR(tddv.date_dt, 'Month-YY')
    order by property_id,month;
 
      """
  
  mysql_query =f"""
    use hummingbird;    
    set @date = '{rds_date}';
        
    WITH cte_reservation as ( SELECT distinct u.property_id,
                concat(if(isnull(p.number), '', concat(p.number, ' - ')), p.name)  as property_name,
                concat(c.first,' ',c.last) as tenant_name, u.id as unit_id, u.number as unit_number, l.id as lease_id, l.status as status, l.start_date as lease_start_date, 1 as day_reservations, l.rent as reservation_rent,
                DATE(r.time) as reservation_date,
                DATE(r.expires) as expiration_date
            
            from reservations r
                join leases l on l.id = r.lease_id
                join units u on u.id = l.unit_id
                join properties p on p.id = u.property_id
                left join leads ld on ld.lease_id = r.lease_id
                left join contacts c on c.id = ld.contact_id
            where u.property_id in ({property_id})
            having reservation_date BETWEEN DATE_FORMAT(DATE_SUB(@date, INTERVAL 1 YEAR), '%Y-%m-01') and @date
            
            )
                SELECT property_id,DATE_FORMAT(cr.reservation_date, '%M-%y') as month, sum(cr.day_reservations) as reservation_count
                FROM cte_reservation cr
                GROUP BY property_id,DATE_FORMAT(cr.reservation_date, '%M-%y')
                ORDER BY property_id,month;
     """
  
  redshift_df = fetch_data(redshift_engine, redshift_query)
  mysql_df = execute_mysql_statements(mysql_engine, mysql_query)


#   mysql_df['date'] = mysql_df['reservation_month'].apply(convert_to_last_day)

  redshift_grouped = redshift_df
  mysql_grouped = mysql_df

  redshift_grouped['month'] = redshift_grouped['month'].str.replace(' ', '', regex=False)

#   redshift_grouped.rename(columns={'month':'date'},inplace=True)  
#   redshift_grouped['date'] = pd.to_datetime(redshift_grouped['date'])
#   redshift_grouped['date'] = redshift_grouped['date'] + pd.offsets.MonthEnd(0)
#   mysql_grouped['date'] = pd.to_datetime(mysql_grouped['date'])

  redshift_grouped = redshift_grouped.add_suffix('_tdw')
  mysql_grouped = mysql_grouped.add_suffix('_rds')
  redshift_grouped.rename(columns={'property_id_tdw':'property_id','month_tdw':'month'},inplace=True)
  mysql_grouped.rename(columns={'property_id_rds':'property_id','month_rds':'month'},inplace=True)
  
  if redshift_df.empty and mysql_df.empty:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','month'], how='outer', indicator=True)
    return comparison_df
  else:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','month'], how='outer', indicator=True)
    comparison_df['_merge'] = comparison_df['_merge'].map({'left_only': 'TDW', 'right_only': 'RDS', 'both': 'Both'})
    comparison_df['m'] = pd.to_datetime(comparison_df['month'],format= '%B-%y')
    comparison_df = comparison_df.sort_values(['property_id','m'],ascending=[True,False])
    comparison_df = comparison_df.drop(columns=['m'])

    comparison_df['Status'] = comparison_df.apply(
        lambda row: 'match' if row['reservation_count_rds'] == row['reservation_count_tdw'] else 'mismatch', axis=1
    )
    return comparison_df
  

def autopay_enrollment(redshift_engine,mysql_engine,tdw_date,rds_date):
  print("autopay enrollment query running")
  date_conditions=setDate_For_Occupancy(tdw_date)

  redshift_query =f"""       
        select tpdv.src_property_id as property_id,
        to_char(dd.date_dt,'Month-YY') as month, 
        count(dd.date_dt) as autopay_count
        from tdw_ro.tdw_occupancy_by_space_pf_v spv 
        join tdw_ro.tdw_property_d_v tpdv on spv.property_sk = tpdv.property_sk
        join tdw_ro.tdw_date_d_v dd ON spv.date_sk = dd.date_sk
        WHERE
        spv.property_sk in ({property_sk}) AND
        ({date_conditions}) AND
        spv.autopay_space_count = 1
        group by tpdv.src_property_id,to_char(dd.date_dt,'Month-YY')
        order by property_id,month;
      """
  
  mysql_query =f"""
    use hummingbird;
    set @date = '{rds_date}';
        
        
    WITH cte_auto_pay AS (
    SELECT
    l.id AS lease_id,
    l.end_date AS lease_end_date,
    lpm.id AS lease_payment_method_id,
    lpm.created_at AS lease_pm_created_at,
    lpm.deleted AS lease_pm_deleted_at,
    p.utc_offset AS property_utc_offset,
    pm.card_end,
    pm.card_type,
    pm.exp_warning,
    u.id AS unit_id,
    u.number AS unit_number,
    u.property_id AS property_id,
    CONCAT(IF(ISNULL(p.number), '', CONCAT(p.number, ' - ')), p.name) AS property_name,
    TRIM(CONCAT(IFNULL(c.first, ''), ' ', IFNULL(c.last, ''))) AS contact_name,
    t.date AS date,
    p.name
    FROM units u
    JOIN leases l ON l.unit_id = u.id
    JOIN leases_payment_methods lpm ON lpm.lease_id = l.id
    JOIN payment_methods pm ON pm.id = lpm.payment_method_id
    JOIN contacts c ON c.id = pm.contact_id
    JOIN properties p ON p.id = u.property_id
    JOIN   (SELECT @date as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 1 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 2 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 3 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 4 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 5 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 6 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 7 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 8 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 9 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 10 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 11 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 12 MONTH)) as date)  t 
    ON l.start_date <= t.date
    AND (l.end_date IS NULL OR l.end_date > t.date)
    AND DATE(CONVERT_TZ(lpm.created_at, "+00:00", p.utc_offset)) <= t.date
    AND (lpm.deleted IS NULL OR DATE(CONVERT_TZ(lpm.deleted, "+00:00", p.utc_offset)) > t.date)
    WHERE
    l.status = 1  and  
    u.property_id in ({property_id})
    )
    SELECT
    property_id,
    DATE_FORMAT(date, '%M-%y') AS month,
    COUNT(DISTINCT unit_id) AS autopay_count
    FROM
    cte_auto_pay
    GROUP BY
    property_id,
    DATE_FORMAT(date, '%M-%y')
    order by  property_id,month;
    """

  redshift_df = fetch_data(redshift_engine, redshift_query)
  mysql_df = execute_mysql_statements(mysql_engine, mysql_query)

#   mysql_df['date'] = mysql_df['month'].apply(convert_to_last_day)

  redshift_grouped = redshift_df
  mysql_grouped = mysql_df

  redshift_grouped['month'] = redshift_grouped['month'].str.replace(' ', '', regex=False)

#   redshift_grouped.rename(columns={'date_dt':'date'},inplace=True)
#   redshift_grouped['date'] = pd.to_datetime(redshift_grouped['date'])
#   mysql_grouped['date'] = pd.to_datetime(mysql_grouped['date'])

  redshift_grouped = redshift_grouped.add_suffix('_tdw')
  mysql_grouped = mysql_grouped.add_suffix('_rds')
  redshift_grouped.rename(columns={'property_id_tdw':'property_id','month_tdw':'month'},inplace=True)
  mysql_grouped.rename(columns={'property_id_rds':'property_id','month_rds':'month'},inplace=True)
  
  if redshift_df.empty and mysql_df.empty:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','month'], how='outer', indicator=True)
    return comparison_df
  else:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','month'], how='outer', indicator=True)
    comparison_df['_merge'] = comparison_df['_merge'].map({'left_only': 'TDW', 'right_only': 'RDS', 'both': 'Both'})
    comparison_df['m'] = pd.to_datetime(comparison_df['month'],format= '%B-%y')
    comparison_df = comparison_df.sort_values(['property_id','m'],ascending=[True,False])
    comparison_df = comparison_df.drop(columns=['m'])

    comparison_df['Status'] = comparison_df.apply(
        lambda row: 'match' if row['autopay_count_rds'] == row['autopay_count_tdw'] else 'mismatch', axis=1
    )
    comparison_df.fillna({'autopay_count_rds': 0, 'autopay_count_tdw': 0}, inplace=True)
    return comparison_df
  

def number_of_leases(redshift_engine,mysql_engine,tdw_date,rds_date):
  print("number of leases query running")
  adjusted_end_date = pd.to_datetime(tdw_date)
  last_day_of_month = adjusted_end_date + pd.offsets.MonthEnd(0)
  dates = [adjusted_end_date]
  for i in range(1, 13):
      date = adjusted_end_date - pd.DateOffset(months=i)
      last_day_of_month = date + pd.offsets.MonthEnd(0)
      dates.append(last_day_of_month)
      
  date_strs = [date.strftime('%Y-%m-%d') for date in dates]
  date_condition = "d1.date_dt IN (" + ", ".join([f"'{date}'" for date in date_strs]) + ")"

  redshift_query =f"""       
  with del as (
  SELECT
        f.space_sk,
        f.property_sk,
        tpdv.src_property_id as property_id,
        f.invoice_amount,
        d1.date_dt,
        DATEDIFF(day, d2.date_dt, d1.date_dt) AS days_unpaid
    FROM
        tdw_ro.tdw_delinquency_pf_v f
    join tdw_ro.tdw_property_d_v tpdv on f.property_sk = tpdv.property_sk
    JOIN tdw_ro.tdw_date_d_v d1 ON f.date_sk = d1.date_sk
    JOIN tdw_ro.tdw_date_d_v d2 ON f.invoice_due_date_sk = d2.date_sk
    WHERE
          {date_condition}
        AND f.property_sk in ({property_sk})
    )    
        SELECT    tt.property_id,    
        to_char(tt.date_dt,'Month-YY') as month,
    SUM(CASE WHEN days_unpaid BETWEEN 0 AND 10 THEN 1 ELSE 0 END) AS delinquent_count_10,
    SUM(CASE WHEN days_unpaid BETWEEN 11 AND 30 THEN 1 ELSE 0 END) AS delinquent_count_30,
    SUM(CASE WHEN days_unpaid BETWEEN 31 AND 60 THEN 1 ELSE 0 END) AS delinquent_count_60,
    SUM(CASE WHEN days_unpaid BETWEEN 61 AND 90 THEN 1 ELSE 0 END) AS delinquent_count_90,
    SUM(CASE WHEN days_unpaid BETWEEN 91 AND 120 THEN 1 ELSE 0 END) AS delinquent_count_120,
    SUM(CASE WHEN days_unpaid BETWEEN 121 AND 180 THEN 1 ELSE 0 END) AS delinquent_count_180,
    SUM(CASE WHEN days_unpaid BETWEEN 181 AND 360 THEN 1 ELSE 0 END) AS delinquent_count_360,
    SUM(CASE WHEN days_unpaid > 360 THEN 1 ELSE 0 END) AS delinquent_count_gtr_360
    FROM     ( select space_sk,max(days_unpaid) as days_unpaid , property_sk,property_id,date_dt from  del group by space_sk,
    property_sk,property_id,date_dt) tt
    GROUP BY tt.property_id,to_char(tt.date_dt,'Month-YY')
    order by tt.property_id,to_char(tt.date_dt,'Month-YY');
      """
  
  mysql_query =f"""
use hummingbird;
set @date = '{rds_date}';

-- Number of Leases
 with cte_invoices as ( 
             select u.property_id, t.date as report_date, i.lease_id, i.id as invoice_id, i.number as invoice_number,
                 concat(c.first,' ',c.last) as name, u.number as unit_number, l.end_date as move_out_date,
                 i.date as invoice_date, i.due as invoice_due, timestampdiff(day, i.due, t.date) as days_unpaid,
                 (ifnull(i.subtotal, 0) + ifnull(i.total_tax , 0) - ifnull(i.total_discounts ,0)) as total_amount,
                 sum(ifnull(ipb.amount,0)) as total_paid,
                 l.start_date as move_in_date
             from invoices i
                 join leases l on l.id = i.lease_id and l.status = 1
            join (SELECT @date as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 1 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 2 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 3 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 4 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 5 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 6 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 7 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 8 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 9 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 10 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 11 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 12 MONTH)) as date) t
                     on i.due <= t.date and (i.void_date is null or convert(date_format(i.void_date ,'%Y-%m-%d'),DATE) > t.date)
                         and (l.end_date is null or l.end_date > t.date)
                 join units u on u.id = l.unit_id
                 left outer join invoices_payments_breakdown ipb on ipb.invoice_id = i.id and ipb.date <= t.date
                 join contact_leases cl on cl.lease_id = i.lease_id and cl.primary = 1
                 join contacts c on c.id = cl.contact_id
             where u.property_id in ({property_id})
             group by u.property_id, i.lease_id, i.id, days_unpaid, total_amount, t.date
             having ifnull(total_amount, 0) - ifnull(total_paid, 0) > 0
         )
             SELECT property_id, DATE_FORMAT(report_date, '%M-%y') as month,
                 sum(case when days_unpaid between 0 and 10 then 1 else 0 end) as delinquent_count_10,
                 sum(case when days_unpaid between 11 and 30 then 1 else 0 end) as delinquent_count_30,
                 sum(case when days_unpaid between 31 and 60 then 1 else 0 end) as delinquent_count_60,
                 sum(case when days_unpaid between 61 and 90 then 1 else 0 end) as delinquent_count_90,
                 sum(case when days_unpaid between 91 and 120 then 1 else 0 end) as delinquent_count_120,
                 sum(case when days_unpaid between 121 and 180 then 1 else 0 end) as delinquent_count_180,
                 sum(case when days_unpaid between 181 and 360 then 1 else 0 end) as delinquent_count_360,
                 sum(case when days_unpaid > 360 then 1 else 0 end) as delinquent_count_gtr_360
             from (
                 select inv.property_id, inv.report_date, inv.lease_id, max(inv.days_unpaid) as days_unpaid
                 from cte_invoices inv
                 group by inv.property_id, inv.report_date, inv.lease_id
             ) o
             group by property_id, DATE_FORMAT(report_date, '%M-%y')
             order by property_id,month;

     """

  redshift_df = fetch_data(redshift_engine, redshift_query)
  mysql_df = execute_mysql_statements(mysql_engine, mysql_query)

#   mysql_df['date'] = mysql_df['month'].apply(convert_to_last_day)

  redshift_grouped = redshift_df
  mysql_grouped = mysql_df

#   redshift_grouped.rename(columns={'date_dt':'date'},inplace=True)
#   redshift_grouped['date'] = pd.to_datetime(redshift_grouped['date'])
#   mysql_grouped['date'] = pd.to_datetime(mysql_grouped['date'])
  


  redshift_grouped['month'] = redshift_grouped['month'].str.replace(' ', '', regex=False)
  redshift_grouped = redshift_grouped.add_suffix('_tdw')
  mysql_grouped = mysql_grouped.add_suffix('_rds')
  
  redshift_grouped.rename(columns={'property_id_tdw':'property_id','month_tdw':'month'},inplace=True)
  mysql_grouped.rename(columns={'property_id_rds':'property_id','month_rds':'month'},inplace=True)
  
  if redshift_df.empty and mysql_df.empty:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','month'], how='outer', indicator=True)
    return comparison_df
  else:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','month'], how='outer', indicator=True)
    comparison_df['_merge'] = comparison_df['_merge'].map({'left_only': 'TDW', 'right_only': 'RDS', 'both': 'Both'})
    comparison_df['m'] = pd.to_datetime(comparison_df['month'],format='%B-%y')
    comparison_df = comparison_df.sort_values(['property_id','m'],ascending=[True,False])
    comparison_df = comparison_df.drop(columns=['m'])

    comparison_df['Status'] = comparison_df.apply(
        lambda row: 'match' if row['delinquent_count_10_rds'] == row['delinquent_count_10_tdw']
         and row['delinquent_count_30_rds'] == row['delinquent_count_30_tdw']
         and row['delinquent_count_60_rds'] == row['delinquent_count_60_tdw']
         and row['delinquent_count_90_rds'] == row['delinquent_count_90_tdw']
         and row['delinquent_count_120_rds'] == row['delinquent_count_120_tdw']
         and row['delinquent_count_180_rds'] == row['delinquent_count_180_tdw']
         and row['delinquent_count_360_rds'] == row['delinquent_count_360_tdw']
         and row['delinquent_count_gtr_360_rds'] == row['delinquent_count_gtr_360_tdw']
          else 'mismatch', axis=1
    )
    return comparison_df
    

def invoices_accural_income(redshift_engine,mysql_engine,tdw_date,rds_date):
  print("invoices and accural income query running")
  adjusted_end_date,first_day_of_month = setDate_M_D_Y(tdw_date)

  redshift_query =f"""   
    
    select tpdv2.src_property_id as property_id, 
        TO_CHAR( date_dt, 'Month-YY') as revenue_month,
        -- TO_CHAR(DATE_TRUNC('month', date_dt), 'MM-YYYY') as month,
        round(sum(case when tpdv.product_type_cd= 'rent' then round(rev_amt,2) else 0 end),2) as rent,
        round(sum(case when tpdv.product_type_cd = 'insurance' then round(rev_amt,2) else 0 end),2) as insurance,
        round(sum(case when tpdv.product_type_cd in('fee', 'late')   then round(rev_amt,2) else 0 end),2) as fee,
        round(sum(case when tpdv.product_type_cd in('merchandise', 'product') then round(rev_amt,2) else 0 end),2) as merchandise,
        round(sum(case when tpdv.product_type_cd in('deposit', 'cleaning', 'security') then round(rev_amt,2) else 0 end),2) as deposit,
        round(sum(case when tpdv.product_type_cd = 'auction' then round(rev_amt,2) else 0 end),2) as auction , sum(tax) as tax
    from tdw_ro.tdw_summarized_invoices_v s
    join tdw_ro.tdw_product_d_v tpdv on tpdv.product_sk = s.product_sk
    join tdw_ro.tdw_tenant_d_v t on t.tenant_sk = s.tenant_sk
    join tdw_ro.tdw_property_d_v tpdv2 on tpdv2.property_sk = s.property_sk
    join tdw_ro.tdw_space_d_v tsdv on tsdv.space_sk = s.space_sk 
    join tdw_ro.tdw_date_d_v tddv   on s.date_sk  = tddv.date_sk 
    where 
    tpdv2.property_sk in ({property_sk})
    and tddv.date_dt between '{first_day_of_month}' and '{adjusted_end_date}'
    group by tpdv2.src_property_id,TO_CHAR( date_dt, 'Month-YY')
    order by property_id,revenue_month;
        """
  
  mysql_query =f"""
use hummingbird;  
set @date = '{rds_date}';

-- Invoices and accural
with cte_invoice_lines as ( 
    with cte_AR_invoices as (
        select *, 'invoice' as sign, due as base_date from invoices
        where due between DATE_FORMAT(DATE_SUB(@date, INTERVAL 1 YEAR), '%Y-%m-01') and @date
        and (void_date is null or void_date >= due)
        and property_id in ({property_id})
        UNION ALL
        select *, 'void' as sign, void_date as base_date from invoices
        where void_date is not null
        and void_date between DATE_FORMAT(DATE_SUB(@date, INTERVAL 1 YEAR), '%Y-%m-01') and @date
        and void_date >= due
        and property_id in ({property_id})
    ),
    cte_invoices_lines as (
        select il.id, i.id as invoice_id, i.sign as sign, (if(i.sign = 'void', -1, 1) * round(((il.qty * il.cost) - il.total_discounts), 2)) as amount, 'line' as line_type, i.base_date as date, i.void_date, i.property_id, pr.default_type as product_default_type,
        CASE
            WHEN pr.default_type = 'rent' THEN 'rent'
            WHEN pr.default_type = 'product' THEN 'merchandise'
            WHEN pr.default_type = 'late' THEN 'fee'
            WHEN pr.default_type = 'insurance' THEN 'insurance'
            WHEN pr.default_type = 'security' or pr.default_type = 'cleaning' THEN 'deposit'
            WHEN pr.default_type = 'auction' THEN 'auction'
            ELSE 'others'
        END as product_type
        from invoice_lines il
        inner join cte_AR_invoices i on i.id = il.invoice_id
        inner join products pr on pr.id = il.product_id
    ),
    cte_lines_tax as (
        select cil.id, cil.invoice_id, cil.sign, (if(cil.sign = 'void', -1, 1) * round(il.total_tax, 2)) as amount, 'tax' as line_type, cil.date, cil.void_date, cil.property_id, cil.product_default_type,
        'tax' as product_type
        from cte_invoices_lines cil
        inner join invoice_lines il on il.id = cil.id
        where il.total_tax > 0
    )
    select cil.invoice_id, cil.amount, cil.line_type, cil.date, cil.void_date, cil.property_id, cil.product_default_type, cil.product_type from cte_invoices_lines cil
    UNION ALL
    select clt.invoice_id, clt.amount, clt.line_type, clt.date, clt.void_date, clt.property_id, clt.product_default_type, clt.product_type from cte_lines_tax clt ),
    mvw_invoices_details_ms as (
        select cil.property_id, cil.date, cil.product_type as product, round(sum(cil.amount),2) as revenue_amount
        from cte_invoice_lines cil
        group by cil.property_id, cil.product_type, cil.date
    )
    select ih.property_id, DATE_FORMAT(ih.date, '%M-%y') as revenue_month,
                sum(case when ih.product= 'rent' then ih.revenue_amount else 0 end) as rent,
            sum(case when ih.product = 'insurance' then ih.revenue_amount else 0 end) as insurance,
            sum(case when ih.product = 'fee'  then ih.revenue_amount else 0 end) as fee,
            sum(case when ih.product = 'merchandise' then ih.revenue_amount else 0 end) as merchandise,
            sum(case when ih.product = 'deposit' then ih.revenue_amount else 0 end) as deposit,
                sum(case when ih.product = 'auction' then ih.revenue_amount else 0 end) as auction,
                sum(case when ih.product = 'tax' then ih.revenue_amount else 0 end) as tax
    from mvw_invoices_details_ms ih
    GROUP BY ih.property_id, DATE_FORMAT(ih.date, '%M-%y')
    ORDER BY property_id,revenue_month;
     """


  redshift_df = fetch_data(redshift_engine, redshift_query)
  mysql_df = execute_mysql_statements_invoices_accural(mysql_engine, mysql_query)

#   mysql_df.rename(columns={'revenue_date':'date'},inplace=True)
#   mysql_df['date'] = mysql_df['date'].apply(convert_to_last_day_of_month)
#   redshift_df.rename(columns={'month':'date'},inplace=True)
#   redshift_df['date'] = redshift_df['date'].apply(convert_to_last_day_by_numeric_month)

  redshift_grouped = redshift_df
  mysql_grouped = mysql_df

  redshift_grouped['revenue_month'] = redshift_grouped['revenue_month'].str.replace(' ', '', regex=False)
  
#   redshift_grouped['date'] = pd.to_datetime(redshift_grouped['date'])
#   mysql_grouped['date'] = pd.to_datetime(mysql_grouped['date'])

  redshift_grouped = redshift_grouped.add_suffix('_tdw')
  mysql_grouped = mysql_grouped.add_suffix('_rds')
  redshift_grouped.rename(columns={'property_id_tdw':'property_id','revenue_month_tdw':'revenue_month'},inplace=True)
  mysql_grouped.rename(columns={'property_id_rds':'property_id','revenue_month_rds':'revenue_month'},inplace=True)
  
  if redshift_df.empty and mysql_df.empty:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','revenue_month'], how='outer', indicator=True)
    return comparison_df
  else:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','revenue_month'], how='outer', indicator=True)
    comparison_df['_merge'] = comparison_df['_merge'].map({'left_only': 'TDW', 'right_only': 'RDS', 'both': 'Both'})
    comparison_df['rm'] = pd.to_datetime(comparison_df['revenue_month'],format='%B-%y')
    comparison_df = comparison_df.sort_values(['property_id','rm'],ascending=[True,False])
    comparison_df = comparison_df.drop(columns=['rm'])

    comparison_df['Status'] = comparison_df.apply(
        lambda row: 'match' if row['rent_tdw'] == row['rent_rds'] and row['insurance_tdw'] == row['insurance_rds'] and 
        row['auction_tdw'] == row['auction_rds'] and row['fee_tdw'] == row['fee_rds'] and row['merchandise_tdw'] == row['merchandise_rds']
        and row['deposit_tdw'] == row['deposit_rds']   else 'mismatch', axis=1
    )
    return comparison_df
  

def Pre_paid_Liabilities(redshift_engine,mysql_engine,tdw_date,rds_date):
  print("Pre-paid liabilities query running")
  adjusted_end_date,first_day_of_month = setDate_Y_M_D_for_PPL(tdw_date)

  redshift_query =f"""   
                      Select prop.src_property_id as property_id,
                      TO_CHAR(adate.date_dt, 'Month-YY') as liability_month,
 				sum(case when  pr.product_type_cd  = 'rent' and tp.allocation_type_cd = 'LINE' then p.allocated_payment_amount else 0 end) as rent,
 				sum(case when  pr.product_type_cd  = 'insurance' and tp.allocation_type_cd = 'LINE' then p.allocated_payment_amount else 0 end) as insurance,
 				sum(case when  pr.product_type_cd  in ('late','fee') and tp.allocation_type_cd = 'LINE' then p.allocated_payment_amount else 0 end) as fee,
 				sum(case when  pr.product_type_cd  in ('merchandise' ,'product') and tp.allocation_type_cd = 'LINE' then p.allocated_payment_amount else 0 end) as merchandise,
 				sum(case when  pr.product_type_cd  in ('deposit','cleaning') and tp.allocation_type_cd = 'LINE' then p.allocated_payment_amount else 0 end) as deposit,
 				sum(case when pr.product_type_cd = 'tax'  or tp.allocation_type_cd = 'TAX' then p.allocated_payment_amount else 0 end) as tax       
 				FROM tdw_ro.tdw_payment_allocation_tf_v p 
                left join tdw_ro.tdw_payment_misc_attr_d_v pp on p.payment_misc_attr_sk=pp.payment_misc_attr_sk
                left join tdw_ro.tdw_payment_allocation_type_d_v tp on tp.payment_allocation_type_sk = p.allocation_type_sk  
                join tdw_ro.tdw_property_d_v prop on prop.property_sk=p.property_sk
                join tdw_ro.tdw_product_d_v pr on pr.product_sk = p.product_sk 
                 left join tdw_ro.tdw_date_d_v pdate on pdate.date_sk=p.payment_date_sk 
                 left join tdw_ro.tdw_date_d_v adate on adate.date_sk=p.allocation_date_sk
                WHERE  pp.payment_status_cd= 1 
                 and pp.prepaid_flg = 'Y' 
                    and date(adate.date_dt) between '{first_day_of_month}' and '{adjusted_end_date}' and prop.property_sk in ({property_sk}) 
                   group by  prop.src_property_id,TO_CHAR(adate.date_dt, 'Month-YY') 
                   order by property_id,liability_month;
      """
  
  mysql_query =f"""
    use hummingbird;  
    set @date = '{rds_date}';

    with cte_prepaid_liability as (
                    
                SELECT i.property_id,
                    case
                        when ila.type = 'tax' then 'tax'
                        when p.default_type = 'rent' then 'rent'
                        when p.default_type = 'insurance' then 'insurance'
                        when p.default_type = 'late' then 'fee'
                        when p.default_type = 'product' then 'merchandise'
                        when p.default_type = 'auction' then 'auction'
                        when p.default_type in ('cleaning', 'security') then 'deposit'
                        else 'others'
                    end as product_type, ila.amount, ila.date
                FROM invoice_lines_allocation ila
                    join invoice_lines il on ila.invoice_line_id = il.id
                    join products p on il.product_id = p.id
                    join invoices i on i.id = il.invoice_id
                WHERE
                    i.property_id in ({property_id})
                    and ila.date  <= DATE_FORMAT(DATE_SUB(@date, INTERVAL 12 MONTH), '%Y-%m-01')
                    and i.due > ila.date				
    UNION ALL

                SELECT i.property_id,
                    case
                        when ila.type = 'tax' then 'tax'
                        when p.default_type = 'rent' then 'rent'
                        when p.default_type = 'insurance' then 'insurance'
                        when p.default_type = 'late' then 'fee'
                        when p.default_type = 'product' then 'merchandise'
                        when p.default_type = 'auction' then 'auction'
                        when p.default_type in ('cleaning', 'security') then 'deposit'
                        else 'others'
                    end as product_type, ila.amount, ila.date
                FROM invoice_lines_allocation ila
                    join invoice_lines il on ila.invoice_line_id = il.id
                    join products p on il.product_id = p.id
                    join invoices i on i.id = il.invoice_id
                WHERE
                    i.property_id in ({property_id})
                    and ila.date between DATE_FORMAT(DATE_SUB(@date, INTERVAL 11 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 11 MONTH))
                    and i.due > ila.date
    
            
    UNION ALL
    
                SELECT i.property_id,
                    case
                        when ila.type = 'tax' then 'tax'
                        when p.default_type = 'rent' then 'rent'
                        when p.default_type = 'insurance' then 'insurance'
                        when p.default_type = 'late' then 'fee'
                        when p.default_type = 'product' then 'merchandise'
                        when p.default_type = 'auction' then 'auction'
                        when p.default_type in ('cleaning', 'security') then 'deposit'
                        else 'others'
                    end as product_type, ila.amount, ila.date
                FROM invoice_lines_allocation ila
                    join invoice_lines il on ila.invoice_line_id = il.id
                    join products p on il.product_id = p.id
                    join invoices i on i.id = il.invoice_id
                WHERE
                    i.property_id in ({property_id})
                    and ila.date between DATE_FORMAT(DATE_SUB(@date, INTERVAL 10 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 10 MONTH))
                    and i.due > ila.date
    
            
    UNION ALL
    
                SELECT i.property_id,
                    case
                        when ila.type = 'tax' then 'tax'
                        when p.default_type = 'rent' then 'rent'
                        when p.default_type = 'insurance' then 'insurance'
                        when p.default_type = 'late' then 'fee'
                        when p.default_type = 'product' then 'merchandise'
                        when p.default_type = 'auction' then 'auction'
                        when p.default_type in ('cleaning', 'security') then 'deposit'
                        else 'others'
                    end as product_type, ila.amount, ila.date
                FROM invoice_lines_allocation ila
                    join invoice_lines il on ila.invoice_line_id = il.id
                    join products p on il.product_id = p.id
                    join invoices i on i.id = il.invoice_id
                WHERE
                    i.property_id in ({property_id})
                    and ila.date between DATE_FORMAT(DATE_SUB(@date, INTERVAL 9 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 9 MONTH))
                    and i.due > ila.date
    
            
    UNION ALL
    
                SELECT i.property_id,
                    case
                        when ila.type = 'tax' then 'tax'
                        when p.default_type = 'rent' then 'rent'
                        when p.default_type = 'insurance' then 'insurance'
                        when p.default_type = 'late' then 'fee'
                        when p.default_type = 'product' then 'merchandise'
                        when p.default_type = 'auction' then 'auction'
                        when p.default_type in ('cleaning', 'security') then 'deposit'
                        else 'others'
                    end as product_type, ila.amount, ila.date
                FROM invoice_lines_allocation ila
                    join invoice_lines il on ila.invoice_line_id = il.id
                    join products p on il.product_id = p.id
                    join invoices i on i.id = il.invoice_id
                WHERE
                    i.property_id in ({property_id})
                    and ila.date between DATE_FORMAT(DATE_SUB(@date, INTERVAL 8 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 8 MONTH))
                    and i.due > ila.date
    
            
    UNION ALL
    
                SELECT i.property_id,
                    case
                        when ila.type = 'tax' then 'tax'
                        when p.default_type = 'rent' then 'rent'
                        when p.default_type = 'insurance' then 'insurance'
                        when p.default_type = 'late' then 'fee'
                        when p.default_type = 'product' then 'merchandise'
                        when p.default_type = 'auction' then 'auction'
                        when p.default_type in ('cleaning', 'security') then 'deposit'
                        else 'others'
                    end as product_type, ila.amount, ila.date
                FROM invoice_lines_allocation ila
                    join invoice_lines il on ila.invoice_line_id = il.id
                    join products p on il.product_id = p.id
                    join invoices i on i.id = il.invoice_id
                WHERE
                    i.property_id in ({property_id})
                    and ila.date between DATE_FORMAT(DATE_SUB(@date, INTERVAL 7 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 7 MONTH))
                    and i.due > ila.date
    
            
    UNION ALL
    
                SELECT i.property_id,
                    case
                        when ila.type = 'tax' then 'tax'
                        when p.default_type = 'rent' then 'rent'
                        when p.default_type = 'insurance' then 'insurance'
                        when p.default_type = 'late' then 'fee'
                        when p.default_type = 'product' then 'merchandise'
                        when p.default_type = 'auction' then 'auction'
                        when p.default_type in ('cleaning', 'security') then 'deposit'
                        else 'others'
                    end as product_type, ila.amount, ila.date
                FROM invoice_lines_allocation ila
                    join invoice_lines il on ila.invoice_line_id = il.id
                    join products p on il.product_id = p.id
                    join invoices i on i.id = il.invoice_id
                WHERE
                    i.property_id in ({property_id})
                    and ila.date between DATE_FORMAT(DATE_SUB(@date, INTERVAL 6 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 6 MONTH))
                    and i.due > ila.date
    
            
    UNION ALL
    
                SELECT i.property_id,
                    case
                        when ila.type = 'tax' then 'tax'
                        when p.default_type = 'rent' then 'rent'
                        when p.default_type = 'insurance' then 'insurance'
                        when p.default_type = 'late' then 'fee'
                        when p.default_type = 'product' then 'merchandise'
                        when p.default_type = 'auction' then 'auction'
                        when p.default_type in ('cleaning', 'security') then 'deposit'
                        else 'others'
                    end as product_type, ila.amount, ila.date
                FROM invoice_lines_allocation ila
                    join invoice_lines il on ila.invoice_line_id = il.id
                    join products p on il.product_id = p.id
                    join invoices i on i.id = il.invoice_id
                WHERE
                    i.property_id in ({property_id})
                    and ila.date between DATE_FORMAT(DATE_SUB(@date, INTERVAL 5 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 5 MONTH))
                    and i.due > ila.date
    
            
    UNION ALL
    
                SELECT i.property_id,
                    case
                        when ila.type = 'tax' then 'tax'
                        when p.default_type = 'rent' then 'rent'
                        when p.default_type = 'insurance' then 'insurance'
                        when p.default_type = 'late' then 'fee'
                        when p.default_type = 'product' then 'merchandise'
                        when p.default_type = 'auction' then 'auction'
                        when p.default_type in ('cleaning', 'security') then 'deposit'
                        else 'others'
                    end as product_type, ila.amount, ila.date
                FROM invoice_lines_allocation ila
                    join invoice_lines il on ila.invoice_line_id = il.id
                    join products p on il.product_id = p.id
                    join invoices i on i.id = il.invoice_id
                WHERE
                    i.property_id in ({property_id})
                    and ila.date between DATE_FORMAT(DATE_SUB(@date, INTERVAL 4 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 4 MONTH))
                    and i.due > ila.date
    
            
    UNION ALL
    
                SELECT i.property_id,
                    case
                        when ila.type = 'tax' then 'tax'
                        when p.default_type = 'rent' then 'rent'
                        when p.default_type = 'insurance' then 'insurance'
                        when p.default_type = 'late' then 'fee'
                        when p.default_type = 'product' then 'merchandise'
                        when p.default_type = 'auction' then 'auction'
                        when p.default_type in ('cleaning', 'security') then 'deposit'
                        else 'others'
                    end as product_type, ila.amount, ila.date
                FROM invoice_lines_allocation ila
                    join invoice_lines il on ila.invoice_line_id = il.id
                    join products p on il.product_id = p.id
                    join invoices i on i.id = il.invoice_id
                WHERE
                    i.property_id in ({property_id})
                    and ila.date between DATE_FORMAT(DATE_SUB(@date, INTERVAL 3 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 3 MONTH))
                    and i.due > ila.date
    
            
    UNION ALL
    
                SELECT i.property_id,
                    case
                        when ila.type = 'tax' then 'tax'
                        when p.default_type = 'rent' then 'rent'
                        when p.default_type = 'insurance' then 'insurance'
                        when p.default_type = 'late' then 'fee'
                        when p.default_type = 'product' then 'merchandise'
                        when p.default_type = 'auction' then 'auction'
                        when p.default_type in ('cleaning', 'security') then 'deposit'
                        else 'others'
                    end as product_type, ila.amount, ila.date
                FROM invoice_lines_allocation ila
                    join invoice_lines il on ila.invoice_line_id = il.id
                    join products p on il.product_id = p.id
                    join invoices i on i.id = il.invoice_id
                WHERE
                    i.property_id in ({property_id})
                    and ila.date between DATE_FORMAT(DATE_SUB(@date, INTERVAL 2 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 2 MONTH))
                    and i.due > ila.date
    
            
    UNION ALL
    
                SELECT i.property_id,
                    case
                        when ila.type = 'tax' then 'tax'
                        when p.default_type = 'rent' then 'rent'
                        when p.default_type = 'insurance' then 'insurance'
                        when p.default_type = 'late' then 'fee'
                        when p.default_type = 'product' then 'merchandise'
                        when p.default_type = 'auction' then 'auction'
                        when p.default_type in ('cleaning', 'security') then 'deposit'
                        else 'others'
                    end as product_type, ila.amount, ila.date
                FROM invoice_lines_allocation ila
                    join invoice_lines il on ila.invoice_line_id = il.id
                    join products p on il.product_id = p.id
                    join invoices i on i.id = il.invoice_id
                WHERE
                    i.property_id in ({property_id})
                    and ila.date between DATE_FORMAT(DATE_SUB(@date, INTERVAL 1 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 1 MONTH))
                    and i.due > ila.date
    
            
    UNION ALL
    
                SELECT i.property_id,
                    case
                        when ila.type = 'tax' then 'tax'
                        when p.default_type = 'rent' then 'rent'
                        when p.default_type = 'insurance' then 'insurance'
                        when p.default_type = 'late' then 'fee'
                        when p.default_type = 'product' then 'merchandise'
                        when p.default_type = 'auction' then 'auction'
                        when p.default_type in ('cleaning', 'security') then 'deposit'
                        else 'others'
                    end as product_type, ila.amount, ila.date
                FROM invoice_lines_allocation ila
                    join invoice_lines il on ila.invoice_line_id = il.id
                    join products p on il.product_id = p.id
                    join invoices i on i.id = il.invoice_id
                WHERE
                    i.property_id in ({property_id})
                    and ila.date between DATE_FORMAT(@date, '%Y-%m-01') and @date
                    and i.due > ila.date
    
            
        )
        
                select property_id,
                    DATE_FORMAT(pl.date, '%M-%y') as liability_month,
                    sum(case when pl.product_type = 'tax' then pl.amount else 0 end) as tax,
                    sum(case when pl.product_type = 'rent' then pl.amount else 0 end) as rent,
                    sum(case when pl.product_type = 'insurance' then pl.amount else 0 end) as insurance,
                    sum(case when pl.product_type = 'fee' then pl.amount else 0 end) as fee,
                    sum(case when pl.product_type = 'merchandise' then pl.amount else 0 end) as merchandise,
                    sum(case when pl.product_type = 'deposit' then pl.amount else 0 end) as deposit
                    from cte_prepaid_liability pl
                    where pl.date between LAST_DAY(DATE_SUB(@date, INTERVAL 12 MONTH)) and @date
                    group by property_id, DATE_FORMAT(pl.date, '%M-%y')
                    order by property_id, liability_month;
     """

  redshift_df = fetch_data(redshift_engine, redshift_query)
  mysql_df = execute_mysql_statements(mysql_engine, mysql_query)

#   mysql_df['date'] = mysql_df['liability_month'].apply(convert_to_last_day)

  redshift_grouped = redshift_df
  mysql_grouped = mysql_df

#   redshift_grouped.rename(columns={'month':'date'},inplace=True)
#   redshift_grouped['date'] = redshift_grouped['date'].apply(convert_to_last_day_of_month)

#   redshift_grouped['date'] = pd.to_datetime(redshift_grouped['date'])
#   mysql_grouped['date'] = pd.to_datetime(mysql_grouped['date'])
  redshift_grouped['liability_month'] = redshift_grouped['liability_month'].str.replace(' ','',regex=False)

  redshift_grouped = redshift_grouped.add_suffix('_tdw')
  mysql_grouped = mysql_grouped.add_suffix('_rds')
  redshift_grouped.rename(columns={'property_id_tdw':'property_id','liability_month_tdw':'liability_month'},inplace=True)
  mysql_grouped.rename(columns={'property_id_rds':'property_id','liability_month_rds':'liability_month'},inplace=True)
  
  if redshift_df.empty and mysql_df.empty:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','liability_month'], how='outer', indicator=True)
    return comparison_df
  else:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','liability_month'], how='outer', indicator=True)
    comparison_df['_merge'] = comparison_df['_merge'].map({'left_only': 'TDW', 'right_only': 'RDS', 'both': 'Both'})
    comparison_df['lm'] = pd.to_datetime(comparison_df['liability_month'],format='%B-%y')
    comparison_df = comparison_df.sort_values(['property_id','lm'],ascending=[True,False])
    comparison_df = comparison_df.drop(columns=['lm'])

    comparison_df['Status'] = comparison_df.apply(
        lambda row: 'match' if row['rent_tdw'] == row['rent_rds'] and row['fee_rds'] == row['fee_tdw']
          and row['insurance_rds'] == row['insurance_tdw'] and row['merchandise_rds'] == row['merchandise_tdw'] 
          and  row['deposit_rds'] == row['deposit_tdw'] and  row['tax_rds'] == row['tax_tdw']  else 'mismatch', axis=1
    )
    return comparison_df
  

def Allowances_Discounts(redshift_engine,mysql_engine,tdw_date,rds_date):
  print("allownce discount query running")
  adjusted_end_date,first_day_of_month = setDate_Y_M_D(tdw_date)
  

  redshift_query =f"""       
    SELECT dd.property_id,
        dd.discount_month,
        cast(SUM(dd.discount_amount) as NUMERIC(10,2)) AS discount_amount
        
    FROM (
        SELECT
            d.property_id,
            CASE
                WHEN discount_flag = 1 THEN TO_CHAR(d.invoice_due_date, 'Month-YY')
                ELSE TO_CHAR(d.invoice_void_date, 'Month-YY')
            END AS discount_month,
            SUM(NVL(total_discount_amount, 0)) AS discount_amount
        FROM (
        SELECT
                DISTINCT 
                i.src_invoice_line_id,
                indd.date_dt AS invoice_due_date,
                prop.src_property_id AS property_id,
                vd.date_dt AS invoice_void_date,
                i.discount AS total_discount_amount,
                1 AS discount_flag
            FROM tdw_ro.tdw_invoice_line_tf_v i
            JOIN tdw_ro.tdw_property_d_v prop ON prop.property_sk = i.property_sk
            JOIN tdw_ro.tdw_date_d_v indd ON indd.date_sk = i.invoice_due_date_sk
            JOIN tdw_ro.tdw_date_d_v vd ON vd.date_sk = i.invoice_voided_date_sk
            WHERE
                prop.property_sk IN ({property_sk})
                AND indd.date_dt BETWEEN '{first_day_of_month}' AND '{adjusted_end_date}'
                AND (vd.date_dt IS NULL OR vd.date_dt >= indd.date_dt)
                AND i.discount>0
    
            UNION ALL
    
            SELECT  
                DISTINCT 
                i.src_invoice_line_id,
                indd.date_dt AS invoice_due_date,
                prop.src_property_id AS property_id,
                vd.date_dt AS invoice_void_date,
                (i.discount * -1) AS total_discount_amount,
                -1 AS discount_flag
            FROM tdw.tdw_invoice_line_tf i
        
            JOIN tdw_ro.tdw_property_d_v prop ON prop.property_sk = i.property_sk
            JOIN tdw_ro.tdw_date_d_v indd ON indd.date_sk = i.invoice_due_date_sk
            JOIN tdw_ro.tdw_date_d_v vd ON vd.date_sk = i.invoice_voided_date_sk
            WHERE
                prop.property_sk IN ({property_sk})
                AND (vd.date_dt IS NOT NULL AND vd.date_dt BETWEEN '{first_day_of_month}' AND '{adjusted_end_date}')
                AND (vd.date_dt >= indd.date_dt)
        
        ) d
        GROUP BY
            d.property_id,
            discount_month,
            d.invoice_due_date
    ) dd
    GROUP BY dd.property_id,
        dd.discount_month
    ORDER BY dd.property_id,
        dd.discount_month;
        """
  
  mysql_query = f"""
use hummingbird; 
set @date = '{rds_date}';

-- Discounts/Promotions

with cte_discounts as (
       (SELECT
         concat(c.first,' ',c.last) as contact_name, 
         p2.name as promotion_name, 
         i.date as invoice_date,
         i.due as invoice_due_date,
         i.number as invoice_number, 
         i.property_id as invoice_property_id,
         i.void_date as invoice_void_date,
         (i.subtotal + i.total_tax - i.total_discounts) as invoice_total,
         IFNULL(SUM(dli.amount), 0) as total_discount_amount,
         1 as discount_flag,
         u.number as unit_number,
         p2.label as promotion_type
       FROM units u 
         join leases l on u.id = l.unit_id
         join invoices i on l.id = i.lease_id
         join invoice_lines il on i.id = il.invoice_id
         join discount_line_items dli on il.id = dli.invoice_line_id
         join discounts d2 on dli.discount_id = d2.id
         join promotions p2 on d2.promotion_id = p2.id
         join properties pr on pr.id = i.property_id
         join contacts c on c.id = i.contact_id
       WHERE
         i.property_id in ({property_id})
         and i.due between DATE_FORMAT(@date, '%Y-%m-01') and LAST_DAY(@date)
         and (i.void_date is null or i.void_date >= i.due)
       GROUP BY 
         i.id, p2.id
       )
       union all 
         SELECT
         concat(c.first,' ',c.last) as contact_name, 
         p2.name as promotion_name, 
         i.date as invoice_date,
         i.due as invoice_due_date,
         i.number as invoice_number, 
         i.property_id as invoice_property_id,
         i.void_date as invoice_void_date,
         (i.subtotal + i.total_tax - i.total_discounts) as invoice_total,
         IFNULL(SUM(dli.amount), 0) * -1 as total_discount_amount,
         -1 as discount_flag, 
         u.number as unit_number,
         p2.label as promotion_type
       FROM units u 
         join leases l on u.id = l.unit_id
         join invoices i on l.id = i.lease_id
         join invoice_lines il on i.id = il.invoice_id
         join discount_line_items dli on il.id = dli.invoice_line_id
         join discounts d2 on dli.discount_id = d2.id
         join promotions p2 on d2.promotion_id = p2.id
         join properties pr on pr.id = i.property_id
         join contacts c on c.id = i.contact_id
       WHERE
         i.property_id in ({property_id}) and 
         (i.void_date is not null and i.void_date between DATE_FORMAT(@date, '%Y-%m-01') and LAST_DAY(@date))
         and (i.void_date >= i.due)
       GROUP BY 
         i.id, p2.id
 UNION ALL
       (SELECT
         concat(c.first,' ',c.last) as contact_name, 
         p2.name as promotion_name, 
         i.date as invoice_date,
         i.due as invoice_due_date,
         i.number as invoice_number, 
         i.property_id as invoice_property_id,
         i.void_date as invoice_void_date,
         (i.subtotal + i.total_tax - i.total_discounts) as invoice_total,
         IFNULL(SUM(dli.amount), 0) as total_discount_amount,
         1 as discount_flag,
         u.number as unit_number,
         p2.label as promotion_type
       FROM units u 
         join leases l on u.id = l.unit_id
         join invoices i on l.id = i.lease_id
         join invoice_lines il on i.id = il.invoice_id
         join discount_line_items dli on il.id = dli.invoice_line_id
         join discounts d2 on dli.discount_id = d2.id
         join promotions p2 on d2.promotion_id = p2.id
         join properties pr on pr.id = i.property_id
         join contacts c on c.id = i.contact_id
       WHERE
         i.property_id in ({property_id})
         and i.due between DATE_FORMAT(DATE_SUB(@date, INTERVAL 1 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 1 MONTH))
         and (i.void_date is null or i.void_date >= i.due)
       GROUP BY 
         i.id, p2.id
       )
       union all 
         SELECT
         concat(c.first,' ',c.last) as contact_name, 
         p2.name as promotion_name, 
         i.date as invoice_date,
         i.due as invoice_due_date,
         i.number as invoice_number, 
         i.property_id as invoice_property_id,
         i.void_date as invoice_void_date,
         (i.subtotal + i.total_tax - i.total_discounts) as invoice_total,
         IFNULL(SUM(dli.amount), 0) * -1 as total_discount_amount,
         -1 as discount_flag, 
         u.number as unit_number,
         p2.label as promotion_type
       FROM units u 
         join leases l on u.id = l.unit_id
         join invoices i on l.id = i.lease_id
         join invoice_lines il on i.id = il.invoice_id
         join discount_line_items dli on il.id = dli.invoice_line_id
         join discounts d2 on dli.discount_id = d2.id
         join promotions p2 on d2.promotion_id = p2.id
         join properties pr on pr.id = i.property_id
         join contacts c on c.id = i.contact_id
       WHERE
         i.property_id in ({property_id}) and 
         (i.void_date is not null and i.void_date between DATE_FORMAT(DATE_SUB(@date, INTERVAL 1 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 1 MONTH)))
         and (i.void_date >= i.due)
       GROUP BY 
         i.id, p2.id
 UNION ALL
       (SELECT
         concat(c.first,' ',c.last) as contact_name, 
         p2.name as promotion_name, 
         i.date as invoice_date,
         i.due as invoice_due_date,
         i.number as invoice_number, 
         i.property_id as invoice_property_id,
         i.void_date as invoice_void_date,
         (i.subtotal + i.total_tax - i.total_discounts) as invoice_total,
         IFNULL(SUM(dli.amount), 0) as total_discount_amount,
         1 as discount_flag,
         u.number as unit_number,
         p2.label as promotion_type
       FROM units u 
         join leases l on u.id = l.unit_id
         join invoices i on l.id = i.lease_id
         join invoice_lines il on i.id = il.invoice_id
         join discount_line_items dli on il.id = dli.invoice_line_id
         join discounts d2 on dli.discount_id = d2.id
         join promotions p2 on d2.promotion_id = p2.id
         join properties pr on pr.id = i.property_id
         join contacts c on c.id = i.contact_id
       WHERE
         i.property_id in ({property_id})
         and i.due between DATE_FORMAT(DATE_SUB(@date, INTERVAL 2 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 2 MONTH))
         and (i.void_date is null or i.void_date >= i.due)
       GROUP BY 
         i.id, p2.id
       )
       union all 
         SELECT
         concat(c.first,' ',c.last) as contact_name, 
         p2.name as promotion_name, 
         i.date as invoice_date,
         i.due as invoice_due_date,
         i.number as invoice_number, 
         i.property_id as invoice_property_id,
         i.void_date as invoice_void_date,
         (i.subtotal + i.total_tax - i.total_discounts) as invoice_total,
         IFNULL(SUM(dli.amount), 0) * -1 as total_discount_amount,
         -1 as discount_flag, 
         u.number as unit_number,
         p2.label as promotion_type
       FROM units u 
         join leases l on u.id = l.unit_id
         join invoices i on l.id = i.lease_id
         join invoice_lines il on i.id = il.invoice_id
         join discount_line_items dli on il.id = dli.invoice_line_id
         join discounts d2 on dli.discount_id = d2.id
         join promotions p2 on d2.promotion_id = p2.id
         join properties pr on pr.id = i.property_id
         join contacts c on c.id = i.contact_id
       WHERE
         i.property_id in ({property_id}) and 
         (i.void_date is not null and i.void_date between DATE_FORMAT(DATE_SUB(@date, INTERVAL 2 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 2 MONTH)))
         and (i.void_date >= i.due)
       GROUP BY 
         i.id, p2.id
 UNION ALL
       (SELECT
         concat(c.first,' ',c.last) as contact_name, 
         p2.name as promotion_name, 
         i.date as invoice_date,
         i.due as invoice_due_date,
         i.number as invoice_number, 
         i.property_id as invoice_property_id,
         i.void_date as invoice_void_date,
         (i.subtotal + i.total_tax - i.total_discounts) as invoice_total,
         IFNULL(SUM(dli.amount), 0) as total_discount_amount,
         1 as discount_flag,
         u.number as unit_number,
         p2.label as promotion_type
       FROM units u 
         join leases l on u.id = l.unit_id
         join invoices i on l.id = i.lease_id
         join invoice_lines il on i.id = il.invoice_id
         join discount_line_items dli on il.id = dli.invoice_line_id
         join discounts d2 on dli.discount_id = d2.id
         join promotions p2 on d2.promotion_id = p2.id
         join properties pr on pr.id = i.property_id
         join contacts c on c.id = i.contact_id
       WHERE
         i.property_id in ({property_id})
         and i.due between DATE_FORMAT(DATE_SUB(@date, INTERVAL 3 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 3 MONTH))
         and (i.void_date is null or i.void_date >= i.due)
       GROUP BY 
         i.id, p2.id
       )
       union all 
         SELECT
         concat(c.first,' ',c.last) as contact_name, 
         p2.name as promotion_name, 
         i.date as invoice_date,
         i.due as invoice_due_date,
         i.number as invoice_number, 
         i.property_id as invoice_property_id,
         i.void_date as invoice_void_date,
         (i.subtotal + i.total_tax - i.total_discounts) as invoice_total,
         IFNULL(SUM(dli.amount), 0) * -1 as total_discount_amount,
         -1 as discount_flag, 
         u.number as unit_number,
         p2.label as promotion_type
       FROM units u 
         join leases l on u.id = l.unit_id
         join invoices i on l.id = i.lease_id
         join invoice_lines il on i.id = il.invoice_id
         join discount_line_items dli on il.id = dli.invoice_line_id
         join discounts d2 on dli.discount_id = d2.id
         join promotions p2 on d2.promotion_id = p2.id
         join properties pr on pr.id = i.property_id
         join contacts c on c.id = i.contact_id
       WHERE
         i.property_id in ({property_id}) and 
         (i.void_date is not null and i.void_date between DATE_FORMAT(DATE_SUB(@date, INTERVAL 3 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 3 MONTH)))
         and (i.void_date >= i.due)
       GROUP BY 
         i.id, p2.id
 UNION ALL
       (SELECT
         concat(c.first,' ',c.last) as contact_name, 
         p2.name as promotion_name, 
         i.date as invoice_date,
         i.due as invoice_due_date,
         i.number as invoice_number, 
         i.property_id as invoice_property_id,
         i.void_date as invoice_void_date,
         (i.subtotal + i.total_tax - i.total_discounts) as invoice_total,
         IFNULL(SUM(dli.amount), 0) as total_discount_amount,
         1 as discount_flag,
         u.number as unit_number,
         p2.label as promotion_type
       FROM units u 
         join leases l on u.id = l.unit_id
         join invoices i on l.id = i.lease_id
         join invoice_lines il on i.id = il.invoice_id
         join discount_line_items dli on il.id = dli.invoice_line_id
         join discounts d2 on dli.discount_id = d2.id
         join promotions p2 on d2.promotion_id = p2.id
         join properties pr on pr.id = i.property_id
         join contacts c on c.id = i.contact_id
       WHERE
         i.property_id in ({property_id})
         and i.due between DATE_FORMAT(DATE_SUB(@date, INTERVAL 4 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 4 MONTH))
         and (i.void_date is null or i.void_date >= i.due)
       GROUP BY 
         i.id, p2.id
       )
       union all 
         SELECT
         concat(c.first,' ',c.last) as contact_name, 
         p2.name as promotion_name, 
         i.date as invoice_date,
         i.due as invoice_due_date,
         i.number as invoice_number, 
         i.property_id as invoice_property_id,
         i.void_date as invoice_void_date,
         (i.subtotal + i.total_tax - i.total_discounts) as invoice_total,
         IFNULL(SUM(dli.amount), 0) * -1 as total_discount_amount,
         -1 as discount_flag, 
         u.number as unit_number,
         p2.label as promotion_type
       FROM units u 
         join leases l on u.id = l.unit_id
         join invoices i on l.id = i.lease_id
         join invoice_lines il on i.id = il.invoice_id
         join discount_line_items dli on il.id = dli.invoice_line_id
         join discounts d2 on dli.discount_id = d2.id
         join promotions p2 on d2.promotion_id = p2.id
         join properties pr on pr.id = i.property_id
         join contacts c on c.id = i.contact_id
       WHERE
         i.property_id in ({property_id}) and 
         (i.void_date is not null and i.void_date between DATE_FORMAT(DATE_SUB(@date, INTERVAL 4 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 4 MONTH)))
         and (i.void_date >= i.due)
       GROUP BY 
         i.id, p2.id
 UNION ALL
       (SELECT
         concat(c.first,' ',c.last) as contact_name, 
         p2.name as promotion_name, 
         i.date as invoice_date,
         i.due as invoice_due_date,
         i.number as invoice_number, 
         i.property_id as invoice_property_id,
         i.void_date as invoice_void_date,
         (i.subtotal + i.total_tax - i.total_discounts) as invoice_total,
         IFNULL(SUM(dli.amount), 0) as total_discount_amount,
         1 as discount_flag,
         u.number as unit_number,
         p2.label as promotion_type
       FROM units u 
         join leases l on u.id = l.unit_id
         join invoices i on l.id = i.lease_id
         join invoice_lines il on i.id = il.invoice_id
         join discount_line_items dli on il.id = dli.invoice_line_id
         join discounts d2 on dli.discount_id = d2.id
         join promotions p2 on d2.promotion_id = p2.id
         join properties pr on pr.id = i.property_id
         join contacts c on c.id = i.contact_id
       WHERE
         i.property_id in ({property_id})
         and i.due between DATE_FORMAT(DATE_SUB(@date, INTERVAL 5 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 5 MONTH))
         and (i.void_date is null or i.void_date >= i.due)
       GROUP BY 
         i.id, p2.id
       )
       union all 
         SELECT
         concat(c.first,' ',c.last) as contact_name, 
         p2.name as promotion_name, 
         i.date as invoice_date,
         i.due as invoice_due_date,
         i.number as invoice_number, 
         i.property_id as invoice_property_id,
         i.void_date as invoice_void_date,
         (i.subtotal + i.total_tax - i.total_discounts) as invoice_total,
         IFNULL(SUM(dli.amount), 0) * -1 as total_discount_amount,
         -1 as discount_flag, 
         u.number as unit_number,
         p2.label as promotion_type
       FROM units u 
         join leases l on u.id = l.unit_id
         join invoices i on l.id = i.lease_id
         join invoice_lines il on i.id = il.invoice_id
         join discount_line_items dli on il.id = dli.invoice_line_id
         join discounts d2 on dli.discount_id = d2.id
         join promotions p2 on d2.promotion_id = p2.id
         join properties pr on pr.id = i.property_id
         join contacts c on c.id = i.contact_id
       WHERE
         i.property_id in ({property_id}) and 
         (i.void_date is not null and i.void_date between DATE_FORMAT(DATE_SUB(@date, INTERVAL 5 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 5 MONTH)))
         and (i.void_date >= i.due)
       GROUP BY 
         i.id, p2.id
 UNION ALL
       (SELECT
         concat(c.first,' ',c.last) as contact_name, 
         p2.name as promotion_name, 
         i.date as invoice_date,
         i.due as invoice_due_date,
         i.number as invoice_number, 
         i.property_id as invoice_property_id,
         i.void_date as invoice_void_date,
         (i.subtotal + i.total_tax - i.total_discounts) as invoice_total,
         IFNULL(SUM(dli.amount), 0) as total_discount_amount,
         1 as discount_flag,
         u.number as unit_number,
         p2.label as promotion_type
       FROM units u 
         join leases l on u.id = l.unit_id
         join invoices i on l.id = i.lease_id
         join invoice_lines il on i.id = il.invoice_id
         join discount_line_items dli on il.id = dli.invoice_line_id
         join discounts d2 on dli.discount_id = d2.id
         join promotions p2 on d2.promotion_id = p2.id
         join properties pr on pr.id = i.property_id
         join contacts c on c.id = i.contact_id
       WHERE
         i.property_id in ({property_id})
         and i.due between DATE_FORMAT(DATE_SUB(@date, INTERVAL 6 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 6 MONTH))
         and (i.void_date is null or i.void_date >= i.due)
       GROUP BY 
         i.id, p2.id
       )
       union all 
         SELECT
         concat(c.first,' ',c.last) as contact_name, 
         p2.name as promotion_name, 
         i.date as invoice_date,
         i.due as invoice_due_date,
         i.number as invoice_number, 
         i.property_id as invoice_property_id,
         i.void_date as invoice_void_date,
         (i.subtotal + i.total_tax - i.total_discounts) as invoice_total,
         IFNULL(SUM(dli.amount), 0) * -1 as total_discount_amount,
         -1 as discount_flag, 
         u.number as unit_number,
         p2.label as promotion_type
       FROM units u 
         join leases l on u.id = l.unit_id
         join invoices i on l.id = i.lease_id
         join invoice_lines il on i.id = il.invoice_id
         join discount_line_items dli on il.id = dli.invoice_line_id
         join discounts d2 on dli.discount_id = d2.id
         join promotions p2 on d2.promotion_id = p2.id
         join properties pr on pr.id = i.property_id
         join contacts c on c.id = i.contact_id
       WHERE
         i.property_id in ({property_id}) and 
         (i.void_date is not null and i.void_date between DATE_FORMAT(DATE_SUB(@date, INTERVAL 6 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 6 MONTH)))
         and (i.void_date >= i.due)
       GROUP BY 
         i.id, p2.id
 UNION ALL
       (SELECT
         concat(c.first,' ',c.last) as contact_name, 
         p2.name as promotion_name, 
         i.date as invoice_date,
         i.due as invoice_due_date,
         i.number as invoice_number, 
         i.property_id as invoice_property_id,
         i.void_date as invoice_void_date,
         (i.subtotal + i.total_tax - i.total_discounts) as invoice_total,
         IFNULL(SUM(dli.amount), 0) as total_discount_amount,
         1 as discount_flag,
         u.number as unit_number,
         p2.label as promotion_type
       FROM units u 
         join leases l on u.id = l.unit_id
         join invoices i on l.id = i.lease_id
         join invoice_lines il on i.id = il.invoice_id
         join discount_line_items dli on il.id = dli.invoice_line_id
         join discounts d2 on dli.discount_id = d2.id
         join promotions p2 on d2.promotion_id = p2.id
         join properties pr on pr.id = i.property_id
         join contacts c on c.id = i.contact_id
       WHERE
         i.property_id in ({property_id})
         and i.due between DATE_FORMAT(DATE_SUB(@date, INTERVAL 7 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 7 MONTH))
         and (i.void_date is null or i.void_date >= i.due)
       GROUP BY 
         i.id, p2.id
       )
       union all 
         SELECT
         concat(c.first,' ',c.last) as contact_name, 
         p2.name as promotion_name, 
         i.date as invoice_date,
         i.due as invoice_due_date,
         i.number as invoice_number, 
         i.property_id as invoice_property_id,
         i.void_date as invoice_void_date,
         (i.subtotal + i.total_tax - i.total_discounts) as invoice_total,
         IFNULL(SUM(dli.amount), 0) * -1 as total_discount_amount,
         -1 as discount_flag, 
         u.number as unit_number,
         p2.label as promotion_type
       FROM units u 
         join leases l on u.id = l.unit_id
         join invoices i on l.id = i.lease_id
         join invoice_lines il on i.id = il.invoice_id
         join discount_line_items dli on il.id = dli.invoice_line_id
         join discounts d2 on dli.discount_id = d2.id
         join promotions p2 on d2.promotion_id = p2.id
         join properties pr on pr.id = i.property_id
         join contacts c on c.id = i.contact_id
       WHERE
         i.property_id in ({property_id}) and 
         (i.void_date is not null and i.void_date between DATE_FORMAT(DATE_SUB(@date, INTERVAL 7 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 7 MONTH)))
         and (i.void_date >= i.due)
       GROUP BY 
         i.id, p2.id
 UNION ALL
       (SELECT
         concat(c.first,' ',c.last) as contact_name, 
         p2.name as promotion_name, 
         i.date as invoice_date,
         i.due as invoice_due_date,
         i.number as invoice_number, 
         i.property_id as invoice_property_id,
         i.void_date as invoice_void_date,
         (i.subtotal + i.total_tax - i.total_discounts) as invoice_total,
         IFNULL(SUM(dli.amount), 0) as total_discount_amount,
         1 as discount_flag,
         u.number as unit_number,
         p2.label as promotion_type
       FROM units u 
         join leases l on u.id = l.unit_id
         join invoices i on l.id = i.lease_id
         join invoice_lines il on i.id = il.invoice_id
         join discount_line_items dli on il.id = dli.invoice_line_id
         join discounts d2 on dli.discount_id = d2.id
         join promotions p2 on d2.promotion_id = p2.id
         join properties pr on pr.id = i.property_id
         join contacts c on c.id = i.contact_id
       WHERE
         i.property_id in ({property_id})
         and i.due between DATE_FORMAT(DATE_SUB(@date, INTERVAL 8 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 8 MONTH))
         and (i.void_date is null or i.void_date >= i.due)
       GROUP BY 
         i.id, p2.id
       )
       union all 
         SELECT
         concat(c.first,' ',c.last) as contact_name, 
         p2.name as promotion_name, 
         i.date as invoice_date,
         i.due as invoice_due_date,
         i.number as invoice_number, 
         i.property_id as invoice_property_id,
         i.void_date as invoice_void_date,
         (i.subtotal + i.total_tax - i.total_discounts) as invoice_total,
         IFNULL(SUM(dli.amount), 0) * -1 as total_discount_amount,
         -1 as discount_flag, 
         u.number as unit_number,
         p2.label as promotion_type
       FROM units u 
         join leases l on u.id = l.unit_id
         join invoices i on l.id = i.lease_id
         join invoice_lines il on i.id = il.invoice_id
         join discount_line_items dli on il.id = dli.invoice_line_id
         join discounts d2 on dli.discount_id = d2.id
         join promotions p2 on d2.promotion_id = p2.id
         join properties pr on pr.id = i.property_id
         join contacts c on c.id = i.contact_id
       WHERE
         i.property_id in ({property_id}) and 
         (i.void_date is not null and i.void_date between DATE_FORMAT(DATE_SUB(@date, INTERVAL 8 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 8 MONTH)))
         and (i.void_date >= i.due)
       GROUP BY 
         i.id, p2.id
 UNION ALL
       (SELECT
         concat(c.first,' ',c.last) as contact_name, 
         p2.name as promotion_name, 
         i.date as invoice_date,
         i.due as invoice_due_date,
         i.number as invoice_number, 
         i.property_id as invoice_property_id,
         i.void_date as invoice_void_date,
         (i.subtotal + i.total_tax - i.total_discounts) as invoice_total,
         IFNULL(SUM(dli.amount), 0) as total_discount_amount,
         1 as discount_flag,
         u.number as unit_number,
         p2.label as promotion_type
       FROM units u 
         join leases l on u.id = l.unit_id
         join invoices i on l.id = i.lease_id
         join invoice_lines il on i.id = il.invoice_id
         join discount_line_items dli on il.id = dli.invoice_line_id
         join discounts d2 on dli.discount_id = d2.id
         join promotions p2 on d2.promotion_id = p2.id
         join properties pr on pr.id = i.property_id
         join contacts c on c.id = i.contact_id
       WHERE
         i.property_id in ({property_id})
         and i.due between DATE_FORMAT(DATE_SUB(@date, INTERVAL 9 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 9 MONTH))
         and (i.void_date is null or i.void_date >= i.due)
       GROUP BY 
         i.id, p2.id
       )
       union all 
         SELECT
         concat(c.first,' ',c.last) as contact_name, 
         p2.name as promotion_name, 
         i.date as invoice_date,
         i.due as invoice_due_date,
         i.number as invoice_number, 
         i.property_id as invoice_property_id,
         i.void_date as invoice_void_date,
         (i.subtotal + i.total_tax - i.total_discounts) as invoice_total,
         IFNULL(SUM(dli.amount), 0) * -1 as total_discount_amount,
         -1 as discount_flag, 
         u.number as unit_number,
         p2.label as promotion_type
       FROM units u 
         join leases l on u.id = l.unit_id
         join invoices i on l.id = i.lease_id
         join invoice_lines il on i.id = il.invoice_id
         join discount_line_items dli on il.id = dli.invoice_line_id
         join discounts d2 on dli.discount_id = d2.id
         join promotions p2 on d2.promotion_id = p2.id
         join properties pr on pr.id = i.property_id
         join contacts c on c.id = i.contact_id
       WHERE
         i.property_id in ({property_id}) and 
         (i.void_date is not null and i.void_date between DATE_FORMAT(DATE_SUB(@date, INTERVAL 9 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 9 MONTH)))
         and (i.void_date >= i.due)
       GROUP BY 
         i.id, p2.id
 UNION ALL
       (SELECT
         concat(c.first,' ',c.last) as contact_name, 
         p2.name as promotion_name, 
         i.date as invoice_date,
         i.due as invoice_due_date,
         i.number as invoice_number, 
         i.property_id as invoice_property_id,
         i.void_date as invoice_void_date,
         (i.subtotal + i.total_tax - i.total_discounts) as invoice_total,
         IFNULL(SUM(dli.amount), 0) as total_discount_amount,
         1 as discount_flag,
         u.number as unit_number,
         p2.label as promotion_type
       FROM units u 
         join leases l on u.id = l.unit_id
         join invoices i on l.id = i.lease_id
         join invoice_lines il on i.id = il.invoice_id
         join discount_line_items dli on il.id = dli.invoice_line_id
         join discounts d2 on dli.discount_id = d2.id
         join promotions p2 on d2.promotion_id = p2.id
         join properties pr on pr.id = i.property_id
         join contacts c on c.id = i.contact_id
       WHERE
         i.property_id in ({property_id})
         and i.due between DATE_FORMAT(DATE_SUB(@date, INTERVAL 10 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 10 MONTH))
         and (i.void_date is null or i.void_date >= i.due)
       GROUP BY 
         i.id, p2.id
       )
       union all 
         SELECT
         concat(c.first,' ',c.last) as contact_name, 
         p2.name as promotion_name, 
         i.date as invoice_date,
         i.due as invoice_due_date,
         i.number as invoice_number, 
         i.property_id as invoice_property_id,
         i.void_date as invoice_void_date,
         (i.subtotal + i.total_tax - i.total_discounts) as invoice_total,
         IFNULL(SUM(dli.amount), 0) * -1 as total_discount_amount,
         -1 as discount_flag, 
         u.number as unit_number,
         p2.label as promotion_type
       FROM units u 
         join leases l on u.id = l.unit_id
         join invoices i on l.id = i.lease_id
         join invoice_lines il on i.id = il.invoice_id
         join discount_line_items dli on il.id = dli.invoice_line_id
         join discounts d2 on dli.discount_id = d2.id
         join promotions p2 on d2.promotion_id = p2.id
         join properties pr on pr.id = i.property_id
         join contacts c on c.id = i.contact_id
       WHERE
         i.property_id in ({property_id}) and 
         (i.void_date is not null and i.void_date between DATE_FORMAT(DATE_SUB(@date, INTERVAL 10 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 10 MONTH)))
         and (i.void_date >= i.due)
       GROUP BY 
         i.id, p2.id
 UNION ALL
       (SELECT
         concat(c.first,' ',c.last) as contact_name, 
         p2.name as promotion_name, 
         i.date as invoice_date,
         i.due as invoice_due_date,
         i.number as invoice_number, 
         i.property_id as invoice_property_id,
         i.void_date as invoice_void_date,
         (i.subtotal + i.total_tax - i.total_discounts) as invoice_total,
         IFNULL(SUM(dli.amount), 0) as total_discount_amount,
         1 as discount_flag,
         u.number as unit_number,
         p2.label as promotion_type
       FROM units u 
         join leases l on u.id = l.unit_id
         join invoices i on l.id = i.lease_id
         join invoice_lines il on i.id = il.invoice_id
         join discount_line_items dli on il.id = dli.invoice_line_id
         join discounts d2 on dli.discount_id = d2.id
         join promotions p2 on d2.promotion_id = p2.id
         join properties pr on pr.id = i.property_id
         join contacts c on c.id = i.contact_id
       WHERE
         i.property_id in ({property_id})
         and i.due between DATE_FORMAT(DATE_SUB(@date, INTERVAL 11 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 11 MONTH))
         and (i.void_date is null or i.void_date >= i.due)
       GROUP BY 
         i.id, p2.id
       )
       union all 
         SELECT
         concat(c.first,' ',c.last) as contact_name, 
         p2.name as promotion_name, 
         i.date as invoice_date,
         i.due as invoice_due_date,
         i.number as invoice_number, 
         i.property_id as invoice_property_id,
         i.void_date as invoice_void_date,
         (i.subtotal + i.total_tax - i.total_discounts) as invoice_total,
         IFNULL(SUM(dli.amount), 0) * -1 as total_discount_amount,
         -1 as discount_flag, 
         u.number as unit_number,
         p2.label as promotion_type
       FROM units u 
         join leases l on u.id = l.unit_id
         join invoices i on l.id = i.lease_id
         join invoice_lines il on i.id = il.invoice_id
         join discount_line_items dli on il.id = dli.invoice_line_id
         join discounts d2 on dli.discount_id = d2.id
         join promotions p2 on d2.promotion_id = p2.id
         join properties pr on pr.id = i.property_id
         join contacts c on c.id = i.contact_id
       WHERE
         i.property_id in ({property_id}) and 
         (i.void_date is not null and i.void_date between DATE_FORMAT(DATE_SUB(@date, INTERVAL 11 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 11 MONTH)))
         and (i.void_date >= i.due)
       GROUP BY 
         i.id, p2.id
 UNION ALL
       (SELECT
         concat(c.first,' ',c.last) as contact_name, 
         p2.name as promotion_name, 
         i.date as invoice_date,
         i.due as invoice_due_date,
         i.number as invoice_number, 
         i.property_id as invoice_property_id,
         i.void_date as invoice_void_date,
         (i.subtotal + i.total_tax - i.total_discounts) as invoice_total,
         IFNULL(SUM(dli.amount), 0) as total_discount_amount,
         1 as discount_flag,
         u.number as unit_number,
         p2.label as promotion_type
       FROM units u 
         join leases l on u.id = l.unit_id
         join invoices i on l.id = i.lease_id
         join invoice_lines il on i.id = il.invoice_id
         join discount_line_items dli on il.id = dli.invoice_line_id
         join discounts d2 on dli.discount_id = d2.id
         join promotions p2 on d2.promotion_id = p2.id
         join properties pr on pr.id = i.property_id
         join contacts c on c.id = i.contact_id
       WHERE
         i.property_id in ({property_id})
         and i.due between DATE_FORMAT(DATE_SUB(@date, INTERVAL 12 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 12 MONTH))
         and (i.void_date is null or i.void_date >= i.due)
       GROUP BY 
         i.id, p2.id
       )
       union all 
         SELECT
         concat(c.first,' ',c.last) as contact_name, 
         p2.name as promotion_name, 
         i.date as invoice_date,
         i.due as invoice_due_date,
         i.number as invoice_number, 
         i.property_id as invoice_property_id,
         i.void_date as invoice_void_date,
         (i.subtotal + i.total_tax - i.total_discounts) as invoice_total,
         IFNULL(SUM(dli.amount), 0) * -1 as total_discount_amount,
         -1 as discount_flag, 
         u.number as unit_number,
         p2.label as promotion_type
       FROM units u 
         join leases l on u.id = l.unit_id
         join invoices i on l.id = i.lease_id
         join invoice_lines il on i.id = il.invoice_id
         join discount_line_items dli on il.id = dli.invoice_line_id
         join discounts d2 on dli.discount_id = d2.id
         join promotions p2 on d2.promotion_id = p2.id
         join properties pr on pr.id = i.property_id
         join contacts c on c.id = i.contact_id
       WHERE
         i.property_id in ({property_id}) and 
         (i.void_date is not null and i.void_date between DATE_FORMAT(DATE_SUB(@date, INTERVAL 12 MONTH), '%Y-%m-01') and LAST_DAY(DATE_SUB(@date, INTERVAL 12 MONTH)))
         and (i.void_date >= i.due)
       GROUP BY 
         i.id, p2.id
       ),
       cte_summarized_discounts as (
         SELECT 
           d.invoice_property_id as property_id, 
           d.invoice_date, 
           d.invoice_due_date, 
           d.invoice_void_date, 
           SUM(IFNULL(total_discount_amount, 0)) as day_discounts,
           d.discount_flag 
         FROM 
           cte_discounts d
         GROUP BY 
           property_id, 
           invoice_date,
           invoice_due_date,
           invoice_void_date,
           discount_flag
       )
       SELECT 
         d.property_id, 
         IF(discount_flag = 1,DATE_FORMAT(d.invoice_due_date, '%M-%y'),DATE_FORMAT(d.invoice_void_date, '%M-%y')) as discount_month,
         SUM(d.day_discounts) as discount_amount 
       FROM
         cte_summarized_discounts d 
       GROUP BY 
         d.property_id, 
         discount_month
 	    ORDER BY
 		  d.property_id, 
         discount_month;
     """

  redshift_df = fetch_data(redshift_engine, redshift_query)
  mysql_df = execute_mysql_statements(mysql_engine, mysql_query)

#   mysql_df['date'] = mysql_df['discount_month'].apply(convert_to_last_day)
#   redshift_df.rename(columns={'discount_month':'date'},inplace=True)
#   redshift_df['date'] = redshift_df['date'].apply(convert_to_last_day_by_numeric_month)

  redshift_grouped = redshift_df
  mysql_grouped = mysql_df

  redshift_grouped['discount_month'] = redshift_grouped['discount_month'].str.replace(' ','',regex=False)

#   redshift_grouped['date'] = pd.to_datetime(redshift_grouped['date'])
#   mysql_grouped['date'] = pd.to_datetime(mysql_grouped['date'])

  redshift_grouped = redshift_grouped.add_suffix('_tdw')
  mysql_grouped = mysql_grouped.add_suffix('_rds')
  redshift_grouped.rename(columns={'property_id_tdw':'property_id','discount_month_tdw':'discount_month'},inplace=True)
  mysql_grouped.rename(columns={'property_id_rds':'property_id','discount_month_rds':'discount_month'},inplace=True)
  
  if redshift_df.empty and mysql_df.empty:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','discount_month'], how='outer', indicator=True)
    return comparison_df
  else:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','discount_month'], how='outer', indicator=True)
    comparison_df['_merge'] = comparison_df['_merge'].map({'left_only': 'TDW', 'right_only': 'RDS', 'both': 'Both'})
    comparison_df['dm'] = pd.to_datetime(comparison_df['discount_month'],format='%B-%y')
    comparison_df = comparison_df.sort_values(['property_id','dm'],ascending=[True,False])
    comparison_df = comparison_df.drop(columns=['dm'])
    comparison_df.fillna({'discount_amount_rds': 0, 'discount_amount_tdw': 0}, inplace=True)

    comparison_df['Status'] = comparison_df.apply(
        lambda row: 'match' if row['discount_amount_rds'] == row['discount_amount_tdw'] else 'mismatch', axis=1
    )
    return comparison_df
  
def Reserved(redshift_engine,mysql_engine,tdw_date,rds_date):
  print("Reserved query running")

  date_conditions=setDate_For_Occupancy(tdw_date)

  redshift_query = f"""       
        select tpdv.src_property_id as property_id,
        to_char(dd.date_dt,'Month-YY') as month, 
        count(dd.date_dt) as reserved_count,
        sum(spv.space_area) as reserved_sqft
        from tdw_ro.tdw_occupancy_by_space_pf_v spv 
        join tdw_ro.tdw_property_d_v tpdv on spv.property_sk = tpdv.property_sk
        join tdw_ro.tdw_date_d_v dd ON spv.date_sk = dd.date_sk
        WHERE
        spv.property_sk in ({property_sk}) AND
        ({date_conditions}) and 
        spv.reserved_space_count = 1
        group by tpdv.src_property_id,to_char(dd.date_dt,'Month-YY')
        order by tpdv.src_property_id,to_char(dd.date_dt,'Month-YY')
      """
  
  mysql_query =f"""
    use hummingbird;
    set @date = '{rds_date}';
    -- Reserved

    WITH reserved_units as (
        SELECT 
            u.property_id, u.id as unit_id,
            l.id as lease_id, l.status as status,
            DATE(CONVERT_TZ ( r.created, '+00:00', p.utc_offset))  as reservation_date, 
            DATE(CONVERT_TZ ( r.expires, '+00:00', p.utc_offset)) as expiration_date,  
            l.start_date as lease_start_date, 
            1 as day_reservations, 
        (cast(
            (
            select (
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.property_type = un.type) and unit_id = un.id), 0) 
                * 
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.property_type = un.type) and unit_id = un.id), 0)
            )
            from units un where un.id = u.id
            )
        as decimal(10,2)))
        as day_sqft,
            l.rent as reservation_rent,
            @date as date
        from reservations r
        join leases l on l.id = r.lease_id 
        join units u on u.id = l.unit_id 
        join properties p on p.id = u.property_id 
        where u.property_id in ({property_id}) 
            and (l.status <> 1 or (l.start_date <= @date and l.status = 1))
        HAVING reservation_date <= @date
            and ifnull(case when l.status <> 1 then expiration_date else l.start_date end, @date )> @date
    UNION ALL
        SELECT 
            u.property_id, u.id as unit_id,
            l.id as lease_id, l.status as status,
            DATE(CONVERT_TZ ( r.created, '+00:00', p.utc_offset))  as reservation_date, 
            DATE(CONVERT_TZ ( r.expires, '+00:00', p.utc_offset)) as expiration_date,  
            l.start_date as lease_start_date, 
            1 as day_reservations, 
            
        (cast(
            (
            select (
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.property_type = un.type) and unit_id = un.id), 0) 
                * 
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.property_type = un.type) and unit_id = un.id), 0)
            )
            from units un where un.id = u.id
            )
        as decimal(10,2)))
        as day_sqft,
            l.rent as reservation_rent,
            LAST_DAY(DATE_SUB(@date, INTERVAL 1 MONTH)) as date
        from reservations r
        join leases l on l.id = r.lease_id 
        join units u on u.id = l.unit_id 
        join properties p on p.id = u.property_id 
        where u.property_id in ({property_id}) 
            and (l.status <> 1 or (l.start_date <= LAST_DAY(DATE_SUB(@date, INTERVAL 1 MONTH)) and l.status = 1))
        HAVING reservation_date <= LAST_DAY(DATE_SUB(@date, INTERVAL 1 MONTH))
            and ifnull(case when l.status <> 1 then expiration_date else l.start_date end, LAST_DAY(DATE_SUB(@date, INTERVAL 1 MONTH)) )> LAST_DAY(DATE_SUB(@date, INTERVAL 1 MONTH))
    UNION ALL
        SELECT 
            u.property_id, u.id as unit_id,
            l.id as lease_id, l.status as status,
            DATE(CONVERT_TZ ( r.created, '+00:00', p.utc_offset))  as reservation_date, 
            DATE(CONVERT_TZ ( r.expires, '+00:00', p.utc_offset)) as expiration_date,  
            l.start_date as lease_start_date, 
            1 as day_reservations, 
        (cast(
            (
            select (
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.property_type = un.type) and unit_id = un.id), 0) 
                * 
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.property_type = un.type) and unit_id = un.id), 0)
            )
            from units un where un.id = u.id
            )
        as decimal(10,2)))
        as day_sqft,
            l.rent as reservation_rent,
            LAST_DAY(DATE_SUB(@date, INTERVAL 2 MONTH)) as date
        from reservations r
        join leases l on l.id = r.lease_id 
        join units u on u.id = l.unit_id 
        join properties p on p.id = u.property_id 
        where u.property_id in ({property_id}) 
            and (l.status <> 1 or (l.start_date <= LAST_DAY(DATE_SUB(@date, INTERVAL 2 MONTH)) and l.status = 1))
        HAVING reservation_date <= LAST_DAY(DATE_SUB(@date, INTERVAL 2 MONTH))
            and ifnull(case when l.status <> 1 then expiration_date else l.start_date end, LAST_DAY(DATE_SUB(@date, INTERVAL 2 MONTH)) )> LAST_DAY(DATE_SUB(@date, INTERVAL 2 MONTH))
    UNION ALL
        SELECT 
            u.property_id, u.id as unit_id,
            l.id as lease_id, l.status as status,
            DATE(CONVERT_TZ ( r.created, '+00:00', p.utc_offset))  as reservation_date, 
            DATE(CONVERT_TZ ( r.expires, '+00:00', p.utc_offset)) as expiration_date,  
            l.start_date as lease_start_date, 
            1 as day_reservations, 
            
        (cast(
            (
            select (
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.property_type = un.type) and unit_id = un.id), 0) 
                * 
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.property_type = un.type) and unit_id = un.id), 0)
            )
            from units un where un.id = u.id
            )
        as decimal(10,2)))
        as day_sqft,
            l.rent as reservation_rent,
            LAST_DAY(DATE_SUB(@date, INTERVAL 3 MONTH)) as date
        from reservations r
        join leases l on l.id = r.lease_id 
        join units u on u.id = l.unit_id 
        join properties p on p.id = u.property_id 
        where u.property_id in ({property_id}) 
            and (l.status <> 1 or (l.start_date <= LAST_DAY(DATE_SUB(@date, INTERVAL 3 MONTH)) and l.status = 1))
        HAVING reservation_date <= LAST_DAY(DATE_SUB(@date, INTERVAL 3 MONTH))
            and ifnull(case when l.status <> 1 then expiration_date else l.start_date end, LAST_DAY(DATE_SUB(@date, INTERVAL 3 MONTH)) )> LAST_DAY(DATE_SUB(@date, INTERVAL 3 MONTH))
    UNION ALL
        SELECT 
            u.property_id, u.id as unit_id,
            l.id as lease_id, l.status as status,
            DATE(CONVERT_TZ ( r.created, '+00:00', p.utc_offset))  as reservation_date, 
            DATE(CONVERT_TZ ( r.expires, '+00:00', p.utc_offset)) as expiration_date,  
            l.start_date as lease_start_date, 
            1 as day_reservations, 
        (cast(
            (
            select (
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.property_type = un.type) and unit_id = un.id), 0) 
                * 
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.property_type = un.type) and unit_id = un.id), 0)
            )
            from units un where un.id = u.id
            )
        as decimal(10,2)))
        as day_sqft,
            l.rent as reservation_rent,
            LAST_DAY(DATE_SUB(@date, INTERVAL 4 MONTH)) as date
        from reservations r
        join leases l on l.id = r.lease_id 
        join units u on u.id = l.unit_id 
        join properties p on p.id = u.property_id 
        where u.property_id in ({property_id}) 
            and (l.status <> 1 or (l.start_date <= LAST_DAY(DATE_SUB(@date, INTERVAL 4 MONTH)) and l.status = 1))
        HAVING reservation_date <= LAST_DAY(DATE_SUB(@date, INTERVAL 4 MONTH))
            and ifnull(case when l.status <> 1 then expiration_date else l.start_date end, LAST_DAY(DATE_SUB(@date, INTERVAL 4 MONTH)) )> LAST_DAY(DATE_SUB(@date, INTERVAL 4 MONTH))
    UNION ALL
        SELECT 
            u.property_id, u.id as unit_id,
            l.id as lease_id, l.status as status,
            DATE(CONVERT_TZ ( r.created, '+00:00', p.utc_offset))  as reservation_date, 
            DATE(CONVERT_TZ ( r.expires, '+00:00', p.utc_offset)) as expiration_date,  
            l.start_date as lease_start_date, 
            1 as day_reservations, 
        (cast(
            (
            select (
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.property_type = un.type) and unit_id = un.id), 0) 
                * 
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.property_type = un.type) and unit_id = un.id), 0)
            )
            from units un where un.id = u.id
            )
        as decimal(10,2)))
        as day_sqft,
            l.rent as reservation_rent,
            LAST_DAY(DATE_SUB(@date, INTERVAL 5 MONTH)) as date
        from reservations r
        join leases l on l.id = r.lease_id 
        join units u on u.id = l.unit_id 
        join properties p on p.id = u.property_id 
        where u.property_id in ({property_id}) 
            and (l.status <> 1 or (l.start_date <= LAST_DAY(DATE_SUB(@date, INTERVAL 5 MONTH)) and l.status = 1))
        HAVING reservation_date <= LAST_DAY(DATE_SUB(@date, INTERVAL 5 MONTH))
            and ifnull(case when l.status <> 1 then expiration_date else l.start_date end, LAST_DAY(DATE_SUB(@date, INTERVAL 5 MONTH)) )> LAST_DAY(DATE_SUB(@date, INTERVAL 5 MONTH))
    UNION ALL
        SELECT 
            u.property_id, u.id as unit_id,
            l.id as lease_id, l.status as status,
            DATE(CONVERT_TZ ( r.created, '+00:00', p.utc_offset))  as reservation_date, 
            DATE(CONVERT_TZ ( r.expires, '+00:00', p.utc_offset)) as expiration_date,  
            l.start_date as lease_start_date, 
            1 as day_reservations, 
        (cast(
            (
            select (
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.property_type = un.type) and unit_id = un.id), 0) 
                * 
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.property_type = un.type) and unit_id = un.id), 0)
            )
            from units un where un.id = u.id
            )
        as decimal(10,2)))
        as day_sqft,
            l.rent as reservation_rent,
            LAST_DAY(DATE_SUB(@date, INTERVAL 6 MONTH)) as date
        from reservations r
        join leases l on l.id = r.lease_id 
        join units u on u.id = l.unit_id 
        join properties p on p.id = u.property_id 
        where u.property_id in ({property_id}) 
            and (l.status <> 1 or (l.start_date <=LAST_DAY(DATE_SUB(@date, INTERVAL 6 MONTH)) and l.status = 1))
        HAVING reservation_date <=LAST_DAY(DATE_SUB(@date, INTERVAL 6 MONTH))
            and ifnull(case when l.status <> 1 then expiration_date else l.start_date end,LAST_DAY(DATE_SUB(@date, INTERVAL 6 MONTH)) )>LAST_DAY(DATE_SUB(@date, INTERVAL 6 MONTH))
    UNION ALL
        SELECT 
            u.property_id, u.id as unit_id,
            l.id as lease_id, l.status as status,
            DATE(CONVERT_TZ ( r.created, '+00:00', p.utc_offset))  as reservation_date, 
            DATE(CONVERT_TZ ( r.expires, '+00:00', p.utc_offset)) as expiration_date,  
            l.start_date as lease_start_date, 
            1 as day_reservations, 
        (cast(
            (
            select (
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.property_type = un.type) and unit_id = un.id), 0) 
                * 
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.property_type = un.type) and unit_id = un.id), 0)
            )
            from units un where un.id = u.id
            )
        as decimal(10,2)))
        as day_sqft,
            l.rent as reservation_rent,
            LAST_DAY(DATE_SUB(@date, INTERVAL 7 MONTH)) as date
        from reservations r
        join leases l on l.id = r.lease_id 
        join units u on u.id = l.unit_id 
        join properties p on p.id = u.property_id 
        where u.property_id in ({property_id}) 
            and (l.status <> 1 or (l.start_date <= LAST_DAY(DATE_SUB(@date, INTERVAL 7 MONTH)) and l.status = 1))
        HAVING reservation_date <= LAST_DAY(DATE_SUB(@date, INTERVAL 7 MONTH))
            and ifnull(case when l.status <> 1 then expiration_date else l.start_date end, LAST_DAY(DATE_SUB(@date, INTERVAL 7 MONTH)) )> LAST_DAY(DATE_SUB(@date, INTERVAL 7 MONTH))
    UNION ALL
        SELECT 
            u.property_id, u.id as unit_id,
            l.id as lease_id, l.status as status,
            DATE(CONVERT_TZ ( r.created, '+00:00', p.utc_offset))  as reservation_date, 
            DATE(CONVERT_TZ ( r.expires, '+00:00', p.utc_offset)) as expiration_date,  
            l.start_date as lease_start_date, 
            1 as day_reservations, 
        (cast(
            (
            select (
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.property_type = un.type) and unit_id = un.id), 0) 
                * 
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.property_type = un.type) and unit_id = un.id), 0)
            )
            from units un where un.id = u.id
            )
        as decimal(10,2)))
        as day_sqft,
            l.rent as reservation_rent,
            LAST_DAY(DATE_SUB(@date, INTERVAL 8 MONTH)) as date
        from reservations r
        join leases l on l.id = r.lease_id 
        join units u on u.id = l.unit_id 
        join properties p on p.id = u.property_id 
        where u.property_id in ({property_id}) 
    
            and (l.status <> 1 or (l.start_date <= LAST_DAY(DATE_SUB(@date, INTERVAL 8 MONTH)) and l.status = 1))
        HAVING reservation_date <= LAST_DAY(DATE_SUB(@date, INTERVAL 8 MONTH))
            and ifnull(case when l.status <> 1 then expiration_date else l.start_date end, LAST_DAY(DATE_SUB(@date, INTERVAL 8 MONTH)) )> LAST_DAY(DATE_SUB(@date, INTERVAL 8 MONTH))
    UNION ALL
        SELECT 
            u.property_id, u.id as unit_id,
            l.id as lease_id, l.status as status,
            DATE(CONVERT_TZ ( r.created, '+00:00', p.utc_offset))  as reservation_date, 
            DATE(CONVERT_TZ ( r.expires, '+00:00', p.utc_offset)) as expiration_date,  
            l.start_date as lease_start_date, 
            1 as day_reservations, 
        (cast(
            (
            select (
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.property_type = un.type) and unit_id = un.id), 0) 
                * 
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.property_type = un.type) and unit_id = un.id), 0)
            )
            from units un where un.id = u.id
            )
        as decimal(10,2)))
        as day_sqft,
            l.rent as reservation_rent,
            LAST_DAY(DATE_SUB(@date, INTERVAL 9 MONTH)) as date
        from reservations r
        join leases l on l.id = r.lease_id 
        join units u on u.id = l.unit_id 
        join properties p on p.id = u.property_id 
        where u.property_id in ({property_id}) 
    
            and (l.status <> 1 or (l.start_date <= LAST_DAY(DATE_SUB(@date, INTERVAL 9 MONTH)) and l.status = 1))
        HAVING reservation_date <= LAST_DAY(DATE_SUB(@date, INTERVAL 9 MONTH))
            and ifnull(case when l.status <> 1 then expiration_date else l.start_date end, LAST_DAY(DATE_SUB(@date, INTERVAL 9 MONTH)) )> LAST_DAY(DATE_SUB(@date, INTERVAL 9 MONTH))
    UNION ALL
        SELECT 
            u.property_id, u.id as unit_id,
            l.id as lease_id, l.status as status,
            DATE(CONVERT_TZ ( r.created, '+00:00', p.utc_offset))  as reservation_date, 
            DATE(CONVERT_TZ ( r.expires, '+00:00', p.utc_offset)) as expiration_date,  
            l.start_date as lease_start_date, 
            1 as day_reservations, 
        (cast(
            (
            select (
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.property_type = un.type) and unit_id = un.id), 0) 
                * 
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.property_type = un.type) and unit_id = un.id), 0)
            )
            from units un where un.id = u.id
            )
        as decimal(10,2)))
        as day_sqft,
            l.rent as reservation_rent,
            LAST_DAY(DATE_SUB(@date, INTERVAL 10 MONTH)) as date
        from reservations r
        join leases l on l.id = r.lease_id 
        join units u on u.id = l.unit_id 
        join properties p on p.id = u.property_id 
        where u.property_id in ({property_id}) 
            and (l.status <> 1 or (l.start_date <= LAST_DAY(DATE_SUB(@date, INTERVAL 10 MONTH)) and l.status = 1))
        HAVING reservation_date <= LAST_DAY(DATE_SUB(@date, INTERVAL 10 MONTH))
            and ifnull(case when l.status <> 1 then expiration_date else l.start_date end, LAST_DAY(DATE_SUB(@date, INTERVAL 10 MONTH)) )> LAST_DAY(DATE_SUB(@date, INTERVAL 10 MONTH))
    UNION ALL
        SELECT 
            u.property_id, u.id as unit_id,
            l.id as lease_id, l.status as status,
            DATE(CONVERT_TZ ( r.created, '+00:00', p.utc_offset))  as reservation_date, 
            DATE(CONVERT_TZ ( r.expires, '+00:00', p.utc_offset)) as expiration_date,  
            l.start_date as lease_start_date, 
            1 as day_reservations, 
        (cast(
            (
            select (
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.property_type = un.type) and unit_id = un.id), 0) 
                * 
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.property_type = un.type) and unit_id = un.id), 0)
            )
            from units un where un.id = u.id
            )
        as decimal(10,2)))
        as day_sqft,
            l.rent as reservation_rent,
            LAST_DAY(DATE_SUB(@date, INTERVAL 11 MONTH)) as date
        from reservations r
        join leases l on l.id = r.lease_id 
        join units u on u.id = l.unit_id 
        join properties p on p.id = u.property_id 
        where u.property_id in ({property_id}) 
            and (l.status <> 1 or (l.start_date <= LAST_DAY(DATE_SUB(@date, INTERVAL 11 MONTH)) and l.status = 1))
        HAVING reservation_date <= LAST_DAY(DATE_SUB(@date, INTERVAL 11 MONTH))
            and ifnull(case when l.status <> 1 then expiration_date else l.start_date end, LAST_DAY(DATE_SUB(@date, INTERVAL 11 MONTH)) )> LAST_DAY(DATE_SUB(@date, INTERVAL 11 MONTH))
    UNION ALL
        SELECT 
            u.property_id, u.id as unit_id,
            l.id as lease_id, l.status as status,
            DATE(CONVERT_TZ ( r.created, '+00:00', p.utc_offset))  as reservation_date, 
            DATE(CONVERT_TZ ( r.expires, '+00:00', p.utc_offset)) as expiration_date,  
            l.start_date as lease_start_date, 
            1 as day_reservations, 
        (cast(
            (
            select (
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.property_type = un.type) and unit_id = un.id), 0) 
                * 
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.property_type = un.type) and unit_id = un.id), 0)
            )
            from units un where un.id = u.id
            )
        as decimal(10,2)))
        as day_sqft,
            l.rent as reservation_rent,
            LAST_DAY(DATE_SUB(@date, INTERVAL 12 MONTH)) as date
        from reservations r
        join leases l on l.id = r.lease_id 
        join units u on u.id = l.unit_id 
        join properties p on p.id = u.property_id 
        where u.property_id in ({property_id})
            and (l.status <> 1 or (l.start_date <= LAST_DAY(DATE_SUB(@date, INTERVAL 12 MONTH)) and l.status = 1))
        HAVING reservation_date <= LAST_DAY(DATE_SUB(@date, INTERVAL 12 MONTH))
            and ifnull(case when l.status <> 1 then expiration_date else l.start_date end, LAST_DAY(DATE_SUB(@date, INTERVAL 12 MONTH)) )> LAST_DAY(DATE_SUB(@date, INTERVAL 12 MONTH)))
        SELECT property_id,
            DATE_FORMAT(ru.date, '%M-%y') as month,
            sum(ru.day_reservations) as reserved_count,
            sum(ru.day_sqft) as reserved_sqft
        FROM reserved_units ru
        GROUP BY ru.property_id, DATE_FORMAT(ru.date, '%M-%y')
        ORDER BY ru.property_id, DATE_FORMAT(ru.date, '%M-%y');
     """

  redshift_df = fetch_data(redshift_engine, redshift_query)
  mysql_df = execute_mysql_statements(mysql_engine, mysql_query)
#   mysql_df['date'] = mysql_df['month'].apply(convert_to_last_day)
  redshift_grouped = redshift_df
  mysql_grouped = mysql_df

  redshift_grouped['month'] = redshift_grouped['month'].str.replace(' ','',regex=False)
#   redshift_grouped.rename(columns={'date_dt':'date'},inplace=True)
#   redshift_grouped['date'] = pd.to_datetime(redshift_grouped['date'])
#   mysql_grouped['date'] = pd.to_datetime(mysql_grouped['date'])
  redshift_grouped = redshift_grouped.add_suffix('_tdw')
  mysql_grouped = mysql_grouped.add_suffix('_rds')
  redshift_grouped.rename(columns={'property_id_tdw':'property_id','month_tdw':'month'},inplace=True)
  mysql_grouped.rename(columns={'property_id_rds':'property_id','month_rds':'month'},inplace=True)
  if redshift_df.empty and mysql_df.empty:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','month'], how='outer', indicator=True)
    return comparison_df
  else:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','month'], how='outer', indicator=True)
    comparison_df['_merge'] = comparison_df['_merge'].map({'left_only': 'TDW', 'right_only': 'RDS', 'both': 'Both'})
    comparison_df['m'] = pd.to_datetime(comparison_df['month'],format='%B-%y')
    comparison_df = comparison_df.sort_values(['property_id','m'],ascending=[True,False])
    comparison_df = comparison_df.drop(columns=['m'])

    comparison_df.fillna({'reserved_count_tdw': 0, 'reserved_count_rds': 0,'reserved_sqft_tdw':0,'reserved_sqft_rds':0}, inplace=True)
    # Determine the status
    comparison_df['Status'] = comparison_df.apply(
        lambda row: 'match' if row['reserved_count_tdw'] == row['reserved_count_rds']
          and row['reserved_sqft_tdw'] == row['reserved_sqft_rds']
          else 'mismatch', axis=1
    )
    
    return comparison_df
  
def Insurance_Protection_Enrollment(redshift_engine,mysql_engine,tdw_date,rds_date):
  print("insurance protection enrollment query running")
  date_conditions=setDate_For_Occupancy(tdw_date)

  redshift_query =f"""   
    select tpdv.src_property_id as property_id, 
    to_char(dd.date_dt,'Month-YY') as insurance_month, 
    count(distinct space_sk) as insurance_count 
    from 
    tdw_ro.tdw_occupancy_by_space_pf_v spv
    join tdw_ro.tdw_property_d_v tpdv on spv.property_sk = tpdv.property_sk 
    join tdw_ro.tdw_date_d_v dd ON spv.date_sk = dd.date_sk
    WHERE
    spv.property_sk in ({property_sk}) AND
    ({date_conditions}) and 
    spv.insurance_space_count = 1
    group by tpdv.src_property_id,to_char(dd.date_dt,'Month-YY')
    order by tpdv.src_property_id,to_char(dd.date_dt,'Month-YY');
      """
  
  mysql_query =f"""
  use hummingbird;  
  set @date = '{rds_date}';

  -- Insurance/Protection query
  with cte_insurance as (
          
        SELECT 
          CAST(i.coverage AS DECIMAL(10, 2)) as coverage_amount, 
          i.premium_value as premium, 
          l.id as lease_id,
          l.end_date as lease_end_date, 
          s.end_date as service_end_date,
          s.start_date as service_start_date,
          u.property_id as property_id,
          concat(if(isnull(pr.number), '', concat(pr.number, ' - ')), pr.name)  as property_name, 
          u.number as unit_number, 
          u.id as unit_id,
          TRIM(CONCAT(IFNULL(c.first, ''), ' ', IFNULL(c.last, ''))) as contact_name, 
          t.date as date
        FROM units u
          join properties pr on pr.id = u.property_id
          join leases l on l.unit_id = u.id 
          join services s on s.lease_id = l.id 
          join contact_leases cl on l.id = cl.lease_id 
          join contacts c on cl.contact_id = c.id
          join products p on p.id = s.product_id and p.default_type = 'insurance' and s.status = 1
          join insurance i on p.id = i.product_id
          -- join invoices iv on l.id = iv.lease_id join invoice_lines il on iv.id = il.invoice_id
          join (SELECT @date as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 1 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 2 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 3 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 4 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 5 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 6 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 7 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 8 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 9 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 10 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 11 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 12 MONTH)) as date) t
            on t.date between s.start_date and ifnull(s.end_date,ifnull(l.end_date , t.date))
        WHERE l.status = 1 and u.property_id in ({property_id}) 
        GROUP BY 
          l.id, p.id, t.date
        ),
  
        cte_summary_insurance as (
          SELECT
            property_id,
            service_start_date,
            ifnull(service_end_date, lease_end_date) as end_date,
            unit_id,
            date
          FROM
            cte_insurance
          GROUP BY
              unit_id, service_start_date, end_date, date
        )
  
        SELECT
          property_id,
          DATE_FORMAT(date, '%M-%y') as insurance_month,
          count(distinct unit_id) as insurance_count
        FROM
          cte_summary_insurance
        GROUP BY
          property_id,
          DATE_FORMAT(date, '%M-%y')
        order by property_id,
          DATE_FORMAT(date, '%M-%y')
      """
  
  redshift_df = fetch_data(redshift_engine, redshift_query)
  mysql_df = execute_mysql_statements(mysql_engine, mysql_query)

  # mysql_df['date'] = mysql_df['month'].apply(convert_to_last_day)

  redshift_grouped = redshift_df
  mysql_grouped = mysql_df

  # redshift_grouped.rename(columns={'date_dt':'date'},inplace=True)
  # redshift_grouped['date'] = pd.to_datetime(redshift_grouped['date'])
  # mysql_grouped['date'] = pd.to_datetime(mysql_grouped['date'])

  redshift_grouped['insurance_month'] = redshift_grouped['insurance_month'].str.replace(' ','',regex=False)

  redshift_grouped = redshift_grouped.add_suffix('_tdw')
  mysql_grouped = mysql_grouped.add_suffix('_rds')
  redshift_grouped.rename(columns={'property_id_tdw':'property_id','insurance_month_tdw':'insurance_month'},inplace=True)
  mysql_grouped.rename(columns={'property_id_rds':'property_id','insurance_month_rds':'insurance_month'},inplace=True)
  
  if redshift_df.empty and mysql_df.empty:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on= ['property_id','insurance_month'], how='outer', indicator=True)
    return comparison_df
  else:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','insurance_month'], how='outer', indicator=True)
    comparison_df['_merge'] = comparison_df['_merge'].map({'left_only': 'TDW', 'right_only': 'RDS', 'both': 'Both'})
    comparison_df['im'] = pd.to_datetime(comparison_df['insurance_month'],format='%B-%y')
    comparison_df = comparison_df.sort_values(['property_id','im'],ascending=[True,False])
    comparison_df = comparison_df.drop(columns=['im'])

    comparison_df['Status'] = comparison_df.apply(
        lambda row: 'match' if row['insurance_count_tdw'] == row['insurance_count_rds'] else 'mismatch', axis=1
    )
    # comparison_df = comparison_df[~comparison_df['insurance_month'].isin(['August-24','July-24'])]
    # comparison_df.fillna({'insurance_count_tdw': 0, 'insurance_count_rds': 0}, inplace=True)
    return comparison_df
  
def Deposits_by_Product(redshift_engine,mysql_engine,tdw_date,rds_date, tdw_refunds_misc_sk):
  print("deposit by product query running")
  adjusted_end_date,first_day_of_month = setDate_Y_M_D_for_PPL(tdw_date)

  redshift_query =f"""      
      SELECT pd.property_id, TO_CHAR(pd.date, 'Month-YY') AS payment_month,
  SUM(CASE WHEN (pd.product_type = 'security' OR pd.product_type = 'cleaning') AND pd.line_type != 'tax' THEN pd.ila_amount ELSE 0 END) AS deposit,
  SUM(CASE WHEN pd.product_type = 'late' AND pd.line_type != 'tax' THEN pd.ila_amount ELSE 0 END) AS fee,
  SUM(CASE WHEN pd.product_type = 'product' AND pd.line_type != 'tax' THEN pd.ila_amount ELSE 0 END) AS merchandise,
  SUM(CASE WHEN pd.product_type = 'rent' AND pd.line_type != 'tax' THEN pd.ila_amount ELSE 0 END) AS rent,
  SUM(CASE WHEN pd.product_type = 'insurance' AND pd.line_type != 'tax' THEN pd.ila_amount ELSE 0 END) AS insurance,
  SUM(CASE WHEN pd.product_type = 'auction' AND pd.line_type != 'tax' THEN pd.ila_amount ELSE 0 END) AS auction,
  SUM(CASE WHEN pd.line_type = 'tax' THEN pd.ila_amount ELSE 0 END) AS tax
  FROM (
      Select p.src_payment_breakdown_id, p.allocated_payment_amount AS ila_amount,adate.date_dt AS date,
      pp.payment_method_cd AS method,prop.src_property_id AS property_id,
      prod.product_type_cd AS product_type,pat.allocation_type_desc AS line_type
      FROM tdw_ro.tdw_payment_allocation_tf_v p
      JOIN tdw_ro.tdw_payment_misc_attr_d_v pp ON p.payment_misc_attr_sk = pp.payment_misc_attr_sk
      JOIN tdw_ro.tdw_property_d_v prop ON prop.property_sk = p.property_sk
      JOIN tdw_ro.tdw_date_d_v pdate ON pdate.date_sk = p.payment_date_sk
      JOIN tdw_ro.tdw_date_d_v adate ON adate.date_sk = p.allocation_date_sk
      JOIN tdw_ro.tdw_product_d_v prod ON prod.product_sk = p.product_sk
      JOIN tdw_ro.tdw_payment_allocation_type_d_v pat ON pat.payment_allocation_type_sk = p.allocation_type_sk
      JOIN tdw_ro.tdw_refunds_misc_attr_d_v rfm ON p.refunds_misc_attr_sk = rfm.refunds_misc_attr_sk
      WHERE credit_type_desc = 'payment'
      AND lower(pp.payment_method_cd) NOT IN ('credit', 'loss')
      AND pp.payment_status_cd != 0
      AND adate.date_dt BETWEEN '{first_day_of_month}' AND '{adjusted_end_date}'
      -- AND pdate.date_dt = adate.date_dt
      AND  prop.property_sk IN ({property_sk})
      AND p.refunds_misc_attr_sk = {tdw_refunds_misc_sk}
      UNION ALL
      Select 1,p.ila_amount,adate.date_dt AS date,
      p.method AS method,prop.src_property_id AS property_id,
      prod.product_type_cd AS product_type,lower(rat.allocation_type_cd) AS line_type
      FROM tdw_ro.tdw_refunds_tf_v p
      JOIN tdw_ro.tdw_property_d_v prop ON prop.property_sk = p.property_sk
      LEFT JOIN tdw_ro.tdw_date_d_v adate ON adate.date_sk = p.ila_date_sk
      LEFT JOIN tdw_ro.tdw_product_d_v prod ON prod.product_sk = p.product_sk
      LEFT JOIN tdw_ro.tdw_refund_allocation_type_d_v rat ON rat.refund_allocation_type_sk = p.refund_allocation_type_sk
      LEFT JOIN tdw_ro.tdw_refunds_misc_attr_d_v rfm ON p.refunds_misc_attr_sk = rfm.refunds_misc_attr_sk
      WHERE lower(p.method) NOT IN ('credit', 'loss')
      AND p.status != 0
      AND adate.date_dt BETWEEN '{first_day_of_month}' AND '{adjusted_end_date}'
      AND prop.property_sk IN ({property_sk})
      AND p.refunds_misc_attr_sk <> {tdw_refunds_misc_sk}
  ) AS pd
  GROUP BY pd.property_id, TO_CHAR(pd.date, 'Month-YY') 
  ORDER BY pd.property_id,TO_CHAR(pd.date, 'Month-YY');
        """
  
  mysql_query =f"""
  use hummingbird;
  set @date = '{rds_date}';
  
  -- deposit by products
  
  select * from (
      with payment_details as (

      WITH cte_payments as (
      SELECT p.id, concat(c.first, ' ', c.last) as contact_name, u.number as unit_number, ila.amount as ila_amount, date(p.date) as date, ila.date as ila_date, ila.type as ila_type, p.method, p.property_id, p.contact_id,
          ila.invoice_line_id, i.due as invoice_due, pr.default_type as product_type, pr.name as product_name,
          pm.card_type, pm.card_end, p.number,u.type as unit_type,pr.id as product_id,
          ifnull(ppr.income_account_id,pr.income_account_id) as income_account_id,
              ifnull(pr.slug,pr.default_type) as product_slug
      FROM hummingbird.invoice_lines_allocation ila
          inner join hummingbird.invoices_payments_breakdown ipb on ipb.id = ila.invoice_payment_breakdown_id
              and ipb.refund_id is null
          inner join hummingbird.payments p on p.id = ipb.payment_id
          inner join hummingbird.invoices i on i.id = ila.invoice_id
          inner join hummingbird.contacts c on c.id = p.contact_id
          left join hummingbird.invoice_lines il on il.id = ila.invoice_line_id
          left join hummingbird.products pr on pr.id = il.product_id
          left join hummingbird.payment_methods pm on pm.id = p.payment_methods_id
          left join hummingbird.leases l on l.id = i.lease_id
          left join hummingbird.units u on u.id = l.unit_id
          left join hummingbird.property_products ppr on ppr.product_id = pr.id and ppr.property_id = i.property_id
      WHERE  p.credit_type = 'payment'
          and p.method not in ('credit', 'loss')
          and p.status != 0
          and date(ila.date) between DATE_FORMAT(DATE_SUB(DATE(NOW()), INTERVAL 1 YEAR), '%Y-%m-01') and @date
          and p.property_id in ({property_id}) ),
      cte_payment_refund as (
      SELECT p.id, r.id as refund_id, ila.amount as ila_amount, ila.type as ila_type, p.method, ila.date as ila_date, p.property_id, r.refund_to as contact_id,
          ila.invoice_line_id, i.due as invoice_due, pr.default_type as product_type, pr.name as product_name, r.type as refund_type,
          pm.card_type, pm.card_end, p.number, p.date as payment_date,u.type as unit_type,pr.id as product_id,
          ifnull(ppr.income_account_id,pr.income_account_id) as income_account_id,
              ifnull(pr.slug,pr.default_type) as product_slug
      FROM hummingbird.invoice_lines_allocation ila
          inner join hummingbird.invoices_payments_breakdown ipb on ipb.id = ila.invoice_payment_breakdown_id
          inner join hummingbird.payments p on p.id = ipb.payment_id
          inner join hummingbird.invoices i on i.id = ila.invoice_id
          inner join hummingbird.refunds r on r.id = ipb.refund_id
          left join hummingbird.invoice_lines il on il.id = ila.invoice_line_id
          left join hummingbird.products pr on pr.id = il.product_id
          left join hummingbird.payment_methods pm on pm.id = p.payment_methods_id
          left join hummingbird.leases l on l.id = i.lease_id
          left join hummingbird.units u on u.id = l.unit_id
          left join hummingbird.property_products ppr on ppr.product_id = pr.id and ppr.property_id = i.property_id
      WHERE  p.credit_type = 'payment'
          and p.method not in ('credit', 'loss')
          and p.status != 0
          and ila.date between DATE_FORMAT(DATE_SUB(DATE(NOW()), INTERVAL 1 YEAR), '%Y-%m-01') and @date
          and p.property_id in ({property_id}))

      SELECT p.id, p.property_id, p.ila_amount as amount, p.ila_date as date, p.ila_type as line_type, p.product_type,
      'deposit' as deposit_type
      FROM cte_payments p

      UNION ALL

      SELECT pr.id, pr.property_id, pr.ila_amount as amount, pr.ila_date as date, pr.ila_type as line_type, pr.product_type,
      pr.refund_type as deposit_type
      FROM cte_payment_refund pr

      )

      SELECT pd.property_id, DATE_FORMAT(pd.date, '%M-%y') as payment_month,
          sum(case when (pd.product_type = 'security' or pd.product_type = 'cleaning') and pd.line_type != 'tax' then pd.amount else 0 end) as 'deposit',
          sum(case when pd.product_type = 'late' and pd.line_type != 'tax' then pd.amount else 0 end) as 'fee',
          sum(case when pd.product_type = 'product' and pd.line_type != 'tax' then pd.amount else 0 end) as 'merchandise',
          sum(case when pd.product_type = 'rent' and pd.line_type != 'tax' then pd.amount else 0 end) as 'rent',
      sum(case when pd.product_type = 'insurance' and pd.line_type != 'tax' then pd.amount else 0 end) as 'insurance',
          sum(case when pd.product_type = 'auction' and pd.line_type != 'tax' then pd.amount else 0 end) as 'auction',
          sum(case when pd.line_type = 'tax' then pd.amount else 0 end) as tax
      FROM payment_details pd
      GROUP BY pd.property_id, DATE_FORMAT(pd.date, '%M-%y')
      ORDER BY pd.property_id, DATE_FORMAT(pd.date, '%M-%y')
  ) x;
      """

  redshift_df = fetch_data(redshift_engine, redshift_query)
  mysql_df = execute_mysql_statements(mysql_engine, mysql_query)

#   mysql_df['date'] = mysql_df['payment_month'].apply(convert_to_last_day_of_month)
#   redshift_df.rename(columns={'payment_month':'date'},inplace=True)
#   redshift_df['date'] = redshift_df['date'].apply(convert_to_last_day_of_month)


  redshift_grouped = redshift_df
  mysql_grouped = mysql_df

  redshift_grouped['payment_month'] = redshift_grouped['payment_month'].str.replace(' ','',regex=False)

#   redshift_grouped['date'] = pd.to_datetime(redshift_grouped['date'])
#   mysql_grouped['date'] = pd.to_datetime(mysql_grouped['date'])

  redshift_grouped = redshift_grouped.add_suffix('_tdw')
  mysql_grouped = mysql_grouped.add_suffix('_rds')
  redshift_grouped.rename(columns={'property_id_tdw':'property_id','payment_month_tdw':'payment_month'},inplace=True)
  mysql_grouped.rename(columns={'property_id_rds':'property_id','payment_month_rds':'payment_month'},inplace=True)
  
  if redshift_df.empty and mysql_df.empty:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','payment_month'], how='outer', indicator=True)
    return comparison_df
  else:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','payment_month'], how='outer', indicator=True)
    comparison_df['_merge'] = comparison_df['_merge'].map({'left_only': 'TDW', 'right_only': 'RDS', 'both': 'Both'})
    comparison_df['pm'] = pd.to_datetime(comparison_df['payment_month'],format='%B-%y')
    comparison_df = comparison_df.sort_values(['property_id','pm'],ascending=[True,False])
    comparison_df = comparison_df.drop(columns=['pm'])

    comparison_df.fillna({'fee_tdw':0,'deposit_tdw':0,'rent_tdw':0,'insurance_tdw':0,'merchandise_tdw':0,'auction_tdw':0,'tax_tdw':0,
                          'fee_rds':0,'deposit_rds':0,'rent_rds':0,'insurance_rds':0,'merchandise_rds':0,'auction_rds':0,'tax_rds':0}
                          ,inplace=True)
    
    comparison_df = comparison_df[comparison_df['payment_month'] != 'July-24']

    comparison_df['Status'] = comparison_df.apply(
        lambda row: 'match' if row['fee_tdw'] == row['fee_rds'] and row['deposit_tdw'] == row['deposit_rds'] and 
        row['rent_tdw'] == row['rent_rds'] and row['insurance_tdw'] == row['insurance_rds'] and 
        row['merchandise_tdw'] == row['merchandise_rds'] and row['auction_tdw'] == row['auction_rds'] and row['tax_tdw'] == row['tax_rds'] else 'mismatch', axis=1
    )
    return comparison_df

def rent_unchanged_in_past_year(redshift_engine,mysql_engine,tdw_date,rds_date):
    print('Rent unchanged in last 12 months query running')

    redshift_query = f"""
    SELECT property_id,count(1) as no_of_leases
    FROM (
    SELECT
        slt.lease_sk, pd.src_property_id as property_id,
        DATEDIFF(days,std.date_dt,'{tdw_date}') AS days_difference,
        ROW_NUMBER() OVER (PARTITION BY slt.lease_sk ORDER BY std.date_dt DESC) AS rn
    FROM tdw_ro.tdw_services_leases_tf_v slt 
    JOIN tdw_ro.tdw_date_d_v std ON std.date_sk = slt.service_start_date_sk
    join tdw_ro.tdw_date_d_v sed ON sed.date_sk = slt.service_end_date_sk 
    JOIN tdw_ro.tdw_property_d_v pd ON pd.property_sk = slt.property_sk
    JOIN tdw_ro.tdw_service_misc_attr_d_v smt ON smt.service_misc_attr_sk = slt.service_misc_attr_sk 
    join tdw_ro.tdw_lease_d_v ld on ld.lease_sk=slt.lease_sk
    join tdw_ro.tdw_date_d_v lsd on ld.lease_start_date_sk =lsd.date_sk
    join tdw_ro.tdw_date_d_v led on ld.lease_end_date_sk=led.date_sk
     join  tdw_ro.tdw_leases_af_v   ld1
    on ld1.lease_sk=ld.lease_sk
    WHERE smt.service_name = 'Rent'
      AND smt.service_status_cd = 1
      and date('{tdw_date}') between std.date_dt and coalesce(sed.date_dt, '{tdw_date}')
      and pd.property_sk in ({property_sk})
      and '{tdw_date}'::date >lsd.date_dt and  (led.date_dt IS NULL OR led.date_dt >'{tdw_date}')
    ) subquery
    WHERE rn = 1 and days_difference>=365
    group by 1;
    """


    mysql_query = f"""
    select p.id as property_id , count(s.lease_id ) as no_of_leases
    from hummingbird.units u
    join hummingbird.leases l on l.unit_id = u.id
    join hummingbird.services s on s.lease_id = l.id
    join hummingbird.properties p on u.property_id = p.id
    and s.product_id = u.product_id
    and date('{rds_date}') between s.start_date and ifnull(s.end_date, '{rds_date}')
    and DATEDIFF('{rds_date}', s.start_date ) >= 365
    where 
    p.id in ({property_id})
    and l.status = 1
    and l.start_date < '{rds_date}'
    and (l.end_date is null or l.end_date > '{rds_date}')
    group by u.property_id;
    """

    redshift_df = fetch_data(redshift_engine, redshift_query)
    mysql_df = fetch_data(mysql_engine, mysql_query)

    redshift_grouped = redshift_df
    mysql_grouped = mysql_df

    redshift_grouped = redshift_grouped.add_suffix('_tdw')
    mysql_grouped = mysql_grouped.add_suffix('_rds')

    redshift_grouped.rename(columns={'property_id_tdw':'property_id'},inplace=True)
    mysql_grouped.rename(columns={'property_id_rds':'property_id'},inplace=True)


    if redshift_df.empty and mysql_df.empty:
        comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id'], how='outer', indicator=True)
        return comparison_df
    else:
        comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id'], how='outer', indicator=True)
        comparison_df['_merge'] = comparison_df['_merge'].map({'left_only': 'TDW', 'right_only': 'RDS', 'both': 'Both'})

        comparison_df = comparison_df.sort_values(by=['property_id'])
    # Determine the status
        comparison_df['Status'] = comparison_df.apply(
        lambda row: 'match' if row['no_of_leases_tdw'] == row['no_of_leases_rds'] else 'mismatch', axis=1
        )
        
        #comparison_df = comparison_df[comparison_df['user_reviews_count_tdw'].notna()]
        return comparison_df
    
def rent_changed_in_past_year(redshift_engine,mysql_engine,tdw_date,rds_date):
    print('Rent changed in last 12 months query running')

    adjusted_end_date,first_day_of_month = setDate_Y_M_D_for_PPL(tdw_date)

    redshift_query = f"""
    select distinct property_id, TO_CHAR(DATE_TRUNC('month', date_dt), 'Month-YY') as month,
    less_than_six, six_eleven, twelve_seventeen, eighteen_twentyfour, above_twentyfour,
    less_than_six_new_rent_amount, six_eleven_new_rent_amount, twelve_seventeen_new_rent_amount,
    eighteen_twentyfour_new_rent_amount, above_twentyfour_new_rent_amount,
    less_than_six_old_rent_amount, six_eleven_old_rent_amount, twelve_seventeen_old_rent_amount,
    eighteen_twentyfour_old_rent_amount, above_twentyfour_old_rent_amount
    from (
    select pd.src_property_id as property_id,
    dd.date_dt,
    sum(CASE WHEN DATEDIFF(month, d.date_dt, dd.date_dt)  < 6 THEN 1 ELSE 0 END) AS less_than_six,
    SUM(CASE WHEN DATEDIFF(month, d.date_dt, dd.date_dt) BETWEEN 6 AND 11 THEN 1 ELSE 0 END) AS six_eleven,
    SUM(CASE WHEN DATEDIFF(month, d.date_dt, dd.date_dt) BETWEEN 12 AND 17 THEN 1 ELSE 0 END) AS twelve_seventeen,
    SUM(CASE WHEN DATEDIFF(month, d.date_dt, dd.date_dt) BETWEEN 18 AND 24 THEN 1 ELSE 0 END) AS eighteen_twentyfour,
    SUM(CASE WHEN DATEDIFF(month, d.date_dt, dd.date_dt) > 24 THEN 1 ELSE 0 END) AS above_twentyfour,
    SUM(CASE WHEN DATEDIFF(month, d.date_dt, dd.date_dt) < 6 THEN rc.current_rent ELSE 0 END) AS less_than_six_new_rent_amount,
    SUM(CASE WHEN DATEDIFF(month, d.date_dt, dd.date_dt) < 6 THEN rc.previous_rent ELSE 0 END) AS less_than_six_old_rent_amount,
    SUM(CASE WHEN DATEDIFF(month, d.date_dt, dd.date_dt) BETWEEN 6 AND 11 THEN rc.current_rent ELSE 0 END) AS six_eleven_new_rent_amount,
    SUM(CASE WHEN DATEDIFF(month, d.date_dt, dd.date_dt) BETWEEN 6 AND 11 THEN rc.previous_rent ELSE 0 END) AS six_eleven_old_rent_amount,
    SUM(CASE WHEN DATEDIFF(month, d.date_dt, dd.date_dt) BETWEEN 12 AND 17 THEN rc.current_rent ELSE 0 END) as twelve_seventeen_new_rent_amount,
    SUM(CASE WHEN DATEDIFF(month, d.date_dt, dd.date_dt) BETWEEN 12 AND 17 THEN rc.previous_rent ELSE 0 END) AS twelve_seventeen_old_rent_amount,
    SUM(CASE WHEN DATEDIFF(month, d.date_dt, dd.date_dt) BETWEEN 18 AND 24 THEN rc.current_rent ELSE 0 END) as eighteen_twentyfour_new_rent_amount,
    SUM(CASE WHEN DATEDIFF(month, d.date_dt, dd.date_dt) BETWEEN 18 AND 24 THEN rc.previous_rent ELSE 0 END) AS eighteen_twentyfour_old_rent_amount,
    SUM(CASE WHEN DATEDIFF(month, d.date_dt, dd.date_dt) > 24 THEN rc.current_rent ELSE 0 END) as above_twentyfour_new_rent_amount,
    SUM(CASE WHEN DATEDIFF(month, d.date_dt, dd.date_dt) > 24 THEN rc.previous_rent ELSE 0 END) AS above_twentyfour_old_rent_amount
    FROM
    tdw_ro.tdw_rent_change_pf_v rc
    join tdw_ro.tdw_date_d_v d on d.date_sk= rc.last_rent_date_sk
    join tdw_ro.tdw_date_d_v dd on dd.date_sk= rc.date_sk 
    join tdw_ro.tdw_property_d_v pd ON pd.property_sk = rc.property_sk
    join tdw_ro.tdw_lease_d_v tldv  on tldv.lease_sk = rc.lease_sk 
    join tdw_ro.tdw_date_d_v lsd on tldv.lease_start_date_sk = lsd.date_sk
    join tdw_ro.tdw_date_d_v led on tldv.lease_end_date_sk = led.date_sk    
    WHERE rc.property_sk in ({property_sk}) and
    lsd.date_dt < '{tdw_date}'
    and tldv.lease_status_cd = 1
    and (led.date_dt  is null or led.date_dt > '{tdw_date}')
    and
    (dd.date_dt BETWEEN '{first_day_of_month}' AND '{adjusted_end_date}')
    GROUP BY 1,2
    ORDER BY dd.date_dt desc);
    """


    mysql_query = f"""
    use hummingbird;
    set @date = '{rds_date}';

    with cte_rent_change_leases as (select
                TRIM(CONCAT(IFNULL(c.first, ''), ' ', IFNULL(c.last, ''))) as name,
                u.number as unit_number,
                max(s.start_date) as date,
                timestampdiff(day, max(s.start_date), date(t.date)) as days_rent_change,
                s.price as new_rent,
                so.price as old_rent,
                s.price - so.price as variance_amt,
                (s.price - so.price) * 100.00 / so.price  as percentage,
                timestampdiff(month, s.start_date, date(t.date)) as months_rent_change,
                u.property_id,
                concat(if(isnull(p.number), '', concat(p.number, ' - ')), p.name)  as property_name,
                l.id as lease_id,
                s.start_date as last_rent_change,
                t.date as report_date
            from units u
            join properties p on p.id = u.property_id
            join leases l on l.unit_id = u.id and l.status = 1
            join (SELECT @date as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 1 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 2 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 3 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 4 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 5 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 6 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 7 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 8 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 9 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 10 MONTH)) as date UNION ALL SELECT LAST_DAY(DATE_SUB(@date, INTERVAL 11 MONTH)) as date) t
                on l.start_date < t.date and (l.end_date is null or l.end_date > t.date)
            join services s on s.lease_id = l.id
                and s.product_id = u.product_id
                and s.status = 1
                and t.date between s.start_date and ifnull(s.end_date, t.date)
                and s.start_date <> l.start_date
            join services so on so.lease_id = s.lease_id
                and so.product_id = u.product_id
                and so.status = 1
                and timestampdiff(day, date(so.end_date), date(s.start_date)) = 1
            join contact_leases cl on cl.lease_id = l.id and cl.primary = 1
            join contacts c on c.id = cl.contact_id
            where u.property_id in ({property_id})
            group by u.property_id, l.id, t.date)
            select rc.property_id, DATE_FORMAT(rc.report_date, '%M-%y') as month,
                sum(case when rc.months_rent_change <6 then 1 else 0 end) as less_than_six,
                sum(case when rc.months_rent_change between 6 and 11 then 1 else 0 end) as six_eleven,
                sum(case when rc.months_rent_change between 12 and 17 then 1 else 0 end) as twelve_seventeen,
                sum(case when rc.months_rent_change between 18 and 24 then 1 else 0 end) as eighteen_twentyfour,
                sum(case when rc.months_rent_change >24 then 1 else 0 end) as above_twentyfour,
                sum(case when rc.months_rent_change <6 then rc.new_rent else 0 end) as less_than_six_new_rent_amount,
                sum(case when rc.months_rent_change between 6 and 11 then rc.new_rent else 0 end) as six_eleven_new_rent_amount,
                sum(case when rc.months_rent_change between 12 and 17 then rc.new_rent else 0 end) as twelve_seventeen_new_rent_amount,
                sum(case when rc.months_rent_change between 18 and 24 then rc.new_rent else 0 end) as eighteen_twentyfour_new_rent_amount,
                sum(case when rc.months_rent_change >24 then rc.new_rent else 0 end) as above_twentyfour_new_rent_amount,
                sum(case when rc.months_rent_change <6 then rc.old_rent else 0 end) as less_than_six_old_rent_amount,
                sum(case when rc.months_rent_change between 6 and 11 then rc.old_rent else 0 end) as six_eleven_old_rent_amount,
                sum(case when rc.months_rent_change between 12 and 17 then rc.old_rent else 0 end) as twelve_seventeen_old_rent_amount,
                sum(case when rc.months_rent_change between 18 and 24 then rc.old_rent else 0 end) as eighteen_twentyfour_old_rent_amount,
                sum(case when rc.months_rent_change >24 then rc.old_rent else 0 end) as above_twentyfour_old_rent_amount
            from cte_rent_change_leases rc
            group by property_id,
			DATE_FORMAT(rc.report_date, '%M-%y')
    """

    redshift_df = fetch_data(redshift_engine, redshift_query)
    mysql_df = execute_mysql_statements(mysql_engine, mysql_query)

    redshift_grouped = redshift_df
    mysql_grouped = mysql_df

    redshift_grouped['month'] = redshift_grouped['month'].str.replace(' ','',regex=False)

    redshift_grouped = redshift_grouped.add_suffix('_tdw')
    mysql_grouped = mysql_grouped.add_suffix('_rds')

    redshift_grouped.rename(columns={'property_id_tdw':'property_id','month_tdw':'month'},inplace=True)
    mysql_grouped.rename(columns={'property_id_rds':'property_id','month_rds':'month'},inplace=True)


    if redshift_df.empty and mysql_df.empty:
        comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','month'], how='outer', indicator=True)
        return comparison_df
    else:
        comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','month'], how='outer', indicator=True)
        comparison_df['_merge'] = comparison_df['_merge'].map({'left_only': 'TDW', 'right_only': 'RDS', 'both': 'Both'})
        comparison_df['m'] = pd.to_datetime(comparison_df['month'],format='%B-%y')
        comparison_df = comparison_df.sort_values(by=['property_id','m'],ascending=[True,False])
        comparison_df = comparison_df.drop(columns=['m'])
    # Determine the status
        columns_base=['less_than_six','six_eleven','twelve_seventeen','eighteen_twentyfour','above_twentyfour',
                      'less_than_six_new_rent_amount','six_eleven_new_rent_amount','twelve_seventeen_new_rent_amount',
                      'twelve_seventeen_new_rent_amount','eighteen_twentyfour_new_rent_amount','above_twentyfour_new_rent_amount',
                      'less_than_six_old_rent_amount','six_eleven_old_rent_amount','twelve_seventeen_old_rent_amount',
                      'eighteen_twentyfour_old_rent_amount','above_twentyfour_old_rent_amount']
        comparison_df['Status'] = comparison_df.apply(
            lambda row: 'match' if all(row[f'{col}_rds'] == row[f'{col}_tdw'] for col in columns_base)
        else 'mismatch', axis=1)
        # comparison_df['Status'] = comparison_df.apply(
        # lambda row: 'match' if row['no_of_leases_tdw'] == row['no_of_leases_rds'] else 'mismatch', axis=1
        # )
        #comparison_df = comparison_df[comparison_df['user_reviews_count_tdw'].notna()]
        return comparison_df

def converted_leads(redshift_engine,mysql_engine,tdw_date,rds_date):
  print("converted_leads query running")

  adjusted_end_date,first_day_of_month = setDate_M_D_Y(tdw_date)

  redshift_query =f"""       
   select pp.src_property_id as property_id,TO_CHAR( d.date_dt, 'Month-YY') as month, count(*) as leads_count
  from tdw_ro.tdw_leases_af_v l
  join tdw_ro.tdw_date_d_v d on l.move_in_date_sk=d.date_sk
  join tdw_ro.tdw_property_d_v pp on l.property_sk=pp.property_sk
  where l.property_sk  in ({property_sk}) 
  and d.date_dt between '{first_day_of_month}' and '{adjusted_end_date}'
  group by  src_property_id, TO_CHAR( d.date_dt, 'Month-YY')
  order by src_property_id, TO_CHAR( d.date_dt, 'Month-YY'); 
      """
  
  mysql_query =f"""
  use hummingbird;
  set @date = '{rds_date}';

  with cte_converted_leads as (
       select l.id as lease_id, u.property_id, cl.contact_id, l.start_date as lead_start_date
       from contact_leases cl
         join leases l on l.id = cl.lease_id
         join units u on u.id = l.unit_id
       where cl.primary = 1
         and l.status = 1
         and u.property_id in  ({property_id})
         and l.start_date between  DATE_FORMAT(DATE_SUB(@date, INTERVAL 1 YEAR), '%Y-%m-01') and @date
       group by l.id
       )
       SELECT  property_id,
         DATE_FORMAT(lead_start_date, '%M-%y') as month,
         count(contact_id) as leads_count
       FROM
         cte_converted_leads
       GROUP BY
         property_id,
         DATE_FORMAT(lead_start_date, '%M-%y')
       order by  property_id,
         DATE_FORMAT(lead_start_date, '%M-%y')
         ;



     """

  redshift_df = fetch_data(redshift_engine, redshift_query)
  mysql_df = execute_mysql_statements(mysql_engine, mysql_query)

#   mysql_df.rename(columns={'move_in_month':'date'},inplace=True)
#   mysql_df['date'] = mysql_df['date'].apply(convert_to_last_day)

#   redshift_df.rename(columns={'calendar_month':'date'},inplace=True)
#   redshift_df['date'] = redshift_df['date'].apply(convert_to_last_day_by_numeric_month)

  redshift_grouped = redshift_df
  mysql_grouped = mysql_df
  
  redshift_grouped['month'] = redshift_grouped['month'].str.replace(' ', '', regex=False)  

#   redshift_grouped['date'] = pd.to_datetime(redshift_grouped['date'])
#   mysql_grouped['date'] = pd.to_datetime(mysql_grouped['date'])


  # print(redshift_grouped,mysql_grouped)
  redshift_grouped = redshift_grouped.add_suffix('_tdw')
  mysql_grouped = mysql_grouped.add_suffix('_rds')
  redshift_grouped.rename(columns={'property_id_tdw':'property_id','month_tdw':'month'},inplace=True)
  mysql_grouped.rename(columns={'property_id_rds':'property_id','month_rds':'month'},inplace=True)
  
  if redshift_df.empty and mysql_df.empty:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','month'], how='outer', indicator=True)
    return comparison_df
  else:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','month'], how='outer', indicator=True)
    comparison_df['_merge'] = comparison_df['_merge'].map({'left_only': 'TDW', 'right_only': 'RDS', 'both': 'Both'})
    comparison_df['mim'] = pd.to_datetime(comparison_df['month'],format= '%B-%y')
    comparison_df = comparison_df.sort_values(['property_id','mim'],ascending=[True,False])
    comparison_df = comparison_df.drop(columns=['mim'])
    columns_base=['leads_count']
    comparison_df['Status'] = comparison_df.apply(
         lambda row: 'match' if all(row[f'{col}_rds'] == row[f'{col}_tdw'] for col in columns_base)
    else 'mismatch', axis=1
     )
    return comparison_df

def write_off(redshift_engine,mysql_engine,tdw_date,rds_date):
  print("write_off query running")

  adjusted_end_date,first_day_of_month = setDate_M_D_Y(tdw_date)

  redshift_query =f"""       
   select prop.src_property_id as property_id,
        TO_CHAR(DATE_TRUNC('month', d.date_dt), 'Month-YY') as month
       , sum(case when pp.payment_method_cd = 'LOSS' then p.allocated_payment_amount else 0 end) as writeoffs           
        from tdw_ro.tdw_payment_allocation_tf_v p
        join tdw_ro.tdw_date_d_v d on d.date_sk=p.payment_breakdown_date_Sk
        join tdw_ro.tdw_property_d_v prop on prop.property_sk=p.property_sk
        join tdw_ro.tdw_payment_misc_attr_d_v pp on pp.payment_misc_attr_sk=p.payment_misc_attr_sk          
                            where   pp.payment_status_cd = 1
                and d.date_dt BETWEEN  '{first_day_of_month}' and '{adjusted_end_date}'
                and pp.payment_method_cd in ('LOSS', 'CREDIT')  
                and prop.property_sk  in ({property_sk}) 
                          GROUP BY property_id,TO_CHAR(DATE_TRUNC('month', d.date_dt), 'Month-YY')
                        order by property_id, month;
      """
  
  mysql_query =f"""
  use hummingbird;
  set @date = '{rds_date}';
  with cte_credits_and_write_offs as (
       SELECT 
         concat(c.first,' ',c.last) as contact_name, 
         c.id contact_id,
         i.number as invoice_number, 
         i.date as invoice_date,
         i.due as invoice_due,
         (i.subtotal + i.total_tax - i.total_discounts) as invoice_total,
         ila.amount as payment_amount,
         ipb.date as payment_date, 
         py.method as payment_method,
         py.sub_method as payment_sub_method,
         u.number as unit_number,
         py.property_id as payment_property_id,
         py.notes,
         ipb.id as ipb_id,
         ila.type as "ila_type",
         pr.name as "product_name",
         pr.id as "product_id",
         pr.default_type as "product_type",
         u.type as "unit_type",
         ifnull(ppr.income_account_id,pr.income_account_id) as income_account_id,
 		    ifnull(pr.slug,pr.default_type) as product_slug
         FROM invoice_lines_allocation ila
         inner join invoices_payments_breakdown ipb on ipb.id = ila.invoice_payment_breakdown_id
         join payments py on py.id = ipb.payment_id
         join invoices i on i.id = ipb.invoice_id
         join contacts c on c.id = i.contact_id
         join leases l on l.id = i.lease_id
         join units u on u.id = l.unit_id
         left join invoice_lines il on il.id = ila.invoice_line_id
 		    left join products pr on pr.id = il.product_id
         left join property_products ppr on ppr.product_id = pr.id and ppr.property_id = i.property_id 
       WHERE 
         py.status = 1
         and ipb.date BETWEEN  DATE_FORMAT(DATE_SUB(@date, INTERVAL 1 YEAR), '%Y-%m-01') and @date
         and py.method in ('loss', 'credit')  
         and py.property_id in ({property_id})
       ORDER BY 
         ipb.date    
                 ),
                 write_offs_summary as (
                   SELECT 
                       wo.payment_property_id as property_id, 
                       wo.contact_id, 
                       wo.payment_date as write_off_date, 
                       sum(case when wo.payment_method = 'loss' then wo.payment_amount else 0 end) as day_writeoffs
                   FROM 
                     cte_credits_and_write_offs wo
                   GROUP BY 
                     wo.payment_property_id, wo.contact_id, wo.payment_date
                 )
                 SELECT 
                   w.property_id, 
                   DATE_FORMAT(w.write_off_date, '%M-%y') as month,
                   sum(w.day_writeoffs) as writeoffs
                 FROM 
                   write_offs_summary w
                 GROUP BY 
                   w.property_id, month
               ORDER BY   w.property_id, month;
     """

  redshift_df = fetch_data(redshift_engine, redshift_query)
  mysql_df = execute_mysql_statements(mysql_engine, mysql_query)

#   mysql_df.rename(columns={'move_in_month':'date'},inplace=True)
#   mysql_df['date'] = mysql_df['date'].apply(convert_to_last_day)

#   redshift_df.rename(columns={'calendar_month':'date'},inplace=True)
#   redshift_df['date'] = redshift_df['date'].apply(convert_to_last_day_by_numeric_month)

  redshift_grouped = redshift_df
  mysql_grouped = mysql_df
  
  redshift_grouped['month'] = redshift_grouped['month'].str.replace(' ', '', regex=False)  

#   redshift_grouped['date'] = pd.to_datetime(redshift_grouped['date'])
#   mysql_grouped['date'] = pd.to_datetime(mysql_grouped['date'])


  # print(redshift_grouped,mysql_grouped)
  redshift_grouped = redshift_grouped.add_suffix('_tdw')
  mysql_grouped = mysql_grouped.add_suffix('_rds')
  redshift_grouped.rename(columns={'property_id_tdw':'property_id','month_tdw':'month'},inplace=True)
  mysql_grouped.rename(columns={'property_id_rds':'property_id','month_rds':'month'},inplace=True)
  
  if redshift_df.empty and mysql_df.empty:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','month'], how='outer', indicator=True)
    return comparison_df
  else:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','month'], how='outer', indicator=True)
    comparison_df['_merge'] = comparison_df['_merge'].map({'left_only': 'TDW', 'right_only': 'RDS', 'both': 'Both'})
    comparison_df['m'] = pd.to_datetime(comparison_df['month'],format= '%B-%y')
    comparison_df = comparison_df.sort_values(['property_id','m'],ascending=[True,False])
    comparison_df = comparison_df.drop(columns=['m'])
    columns_base=['writeoffs']
    comparison_df['Status'] = comparison_df.apply(
         lambda row: 'match' if all(row[f'{col}_rds'] == row[f'{col}_tdw'] for col in columns_base)
    else 'mismatch', axis=1
     )
    return comparison_df

def total_tasks_created_per_month(redshift_engine,mysql_engine,tdw_date,rds_date):
  print("total_tasks_created_per_month query running")

  adjusted_end_date,first_day_of_month = setDate_M_D_Y(tdw_date)

  redshift_query =f"""       
   select property_id,TO_CHAR(date_dt, 'Month-YY') AS month,count(*) as total_tasks from (
    select distinct tpdv.src_property_id as property_id,
     dt.date_dt,
     ta.src_task_id  from tdw_ro.tdw_tasks_af_v ta
    left join tdw_ro.tdw_property_d_v tpdv on tpdv.property_sk = ta.property_sk
    left join tdw_ro.tdw_date_d_v  dt ON ta.task_created_at_date_sk=dt.date_sk
    where ta.property_sk in ({property_sk})  and      
    dt.date_dt between '{first_day_of_month}' and '{adjusted_end_date}'
    )
    group by property_id,TO_CHAR(date_dt, 'Month-YY')
    order by property_id,TO_CHAR(date_dt, 'Month-YY');

      """
  
  mysql_query =f"""
  use hummingbird;
  set @date = '{rds_date}';

 SELECT t.property_id ,DATE_FORMAT(date, '%M-%y') as month,count(*) as total_tasks from (
    select distinct p.id as property_id,DATE(CONVERT_TZ(t.created_at , "+00:00", IFNULL(p.utc_offset, 'Etc/UTC'))) as date, t.id from hummingbird.tasks t
    inner JOIN task_types tt ON tt.id = t.task_type_id
	LEFT JOIN task_objects tko ON tko.task_id = t.id
	LEFT JOIN properties p ON p.id = tko.property_id
    where p.id in ({property_id}) and     DATE(CONVERT_TZ(t.created_at , "+00:00", IFNULL(p.utc_offset, 'Etc/UTC')))
     between   DATE_FORMAT(DATE_SUB(@date, INTERVAL 1 YEAR), '%Y-%m-01') and @date
    ) t
    group by property_id ,DATE_FORMAT(date, '%M-%y')
    order by property_id ,DATE_FORMAT(date, '%M-%y')
 ;
     """

  redshift_df = fetch_data(redshift_engine, redshift_query)
  mysql_df = execute_mysql_statements(mysql_engine, mysql_query)

#   mysql_df.rename(columns={'move_in_month':'date'},inplace=True)
#   mysql_df['date'] = mysql_df['date'].apply(convert_to_last_day)

#   redshift_df.rename(columns={'calendar_month':'date'},inplace=True)
#   redshift_df['date'] = redshift_df['date'].apply(convert_to_last_day_by_numeric_month)

  redshift_grouped = redshift_df
  mysql_grouped = mysql_df
  
  redshift_grouped['month'] = redshift_grouped['month'].str.replace(' ', '', regex=False)  

#   redshift_grouped['date'] = pd.to_datetime(redshift_grouped['date'])
#   mysql_grouped['date'] = pd.to_datetime(mysql_grouped['date'])


  # print(redshift_grouped,mysql_grouped)
  redshift_grouped = redshift_grouped.add_suffix('_tdw')
  mysql_grouped = mysql_grouped.add_suffix('_rds')
  redshift_grouped.rename(columns={'property_id_tdw':'property_id','month_tdw':'month'},inplace=True)
  mysql_grouped.rename(columns={'property_id_rds':'property_id','month_rds':'month'},inplace=True)
  
  if redshift_df.empty and mysql_df.empty:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','month'], how='outer', indicator=True)
    return comparison_df
  else:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','month'], how='outer', indicator=True)
    comparison_df['_merge'] = comparison_df['_merge'].map({'left_only': 'TDW', 'right_only': 'RDS', 'both': 'Both'})
    comparison_df['m'] = pd.to_datetime(comparison_df['month'],format= '%B-%y')
    comparison_df = comparison_df.sort_values(['property_id','m'],ascending=[True,False])
    comparison_df = comparison_df.drop(columns=['m'])
    columns_base=['total_tasks']
    comparison_df['Status'] = comparison_df.apply(
         lambda row: 'match' if all(row[f'{col}_rds'] == row[f'{col}_tdw'] for col in columns_base)
    else 'mismatch', axis=1
     )
    return comparison_df
  

def total_tasks_due_per_month(redshift_engine,mysql_engine,tdw_date,rds_date):
  print("total_tasks_created_per_month query running")

  adjusted_end_date,first_day_of_month = setDate_M_D_Y(tdw_date)

  redshift_query =f"""       
   select property_id,TO_CHAR(date_dt, 'Month-YY') AS month,count(*) as total_tasks from (
    select distinct tpdv.src_property_id as property_id,
     dt.date_dt ,
     ta.src_task_id  from tdw_ro.tdw_tasks_af_v ta
    left join tdw_ro.tdw_property_d_v tpdv on tpdv.property_sk = ta.property_sk
    left join tdw_ro.tdw_date_d_v  dt ON ta.task_due_date_sk=dt.date_sk
    where ta.property_sk in ({property_sk})  and      
    dt.date_dt between '{first_day_of_month}' and '{adjusted_end_date}'
    )
    group by property_id,TO_CHAR(date_dt, 'Month-YY')
    order by property_id,TO_CHAR(date_dt, 'Month-YY');

      """
  
  mysql_query =f"""
  use hummingbird;
  set @date = '{rds_date}';

  SELECT t.property_id ,DATE_FORMAT(date, '%M-%y') as month,count(*) as total_tasks from (
      select distinct p.id as property_id,DATE(CONVERT_TZ(t.due_date , "+00:00", IFNULL(p.utc_offset, 'Etc/UTC'))) as date, t.id from hummingbird.tasks t
      inner JOIN task_types tt ON tt.id = t.task_type_id
    LEFT JOIN task_objects tko ON tko.task_id = t.id
    LEFT JOIN properties p ON p.id = tko.property_id
      where p.id in ({property_id}) and     DATE(CONVERT_TZ(t.due_date , "+00:00", IFNULL(p.utc_offset, 'Etc/UTC')))
      between   DATE_FORMAT(DATE_SUB(@date, INTERVAL 1 YEAR), '%Y-%m-01') and @date
      ) t
      group by property_id ,DATE_FORMAT(date, '%M-%y')
      order by property_id ,DATE_FORMAT(date, '%M-%y')
 ;
     """

  redshift_df = fetch_data(redshift_engine, redshift_query)
  mysql_df = execute_mysql_statements(mysql_engine, mysql_query)

#   mysql_df.rename(columns={'move_in_month':'date'},inplace=True)
#   mysql_df['date'] = mysql_df['date'].apply(convert_to_last_day)

#   redshift_df.rename(columns={'calendar_month':'date'},inplace=True)
#   redshift_df['date'] = redshift_df['date'].apply(convert_to_last_day_by_numeric_month)

  redshift_grouped = redshift_df
  mysql_grouped = mysql_df
  
  redshift_grouped['month'] = redshift_grouped['month'].str.replace(' ', '', regex=False)  

#   redshift_grouped['date'] = pd.to_datetime(redshift_grouped['date'])
#   mysql_grouped['date'] = pd.to_datetime(mysql_grouped['date'])


  # print(redshift_grouped,mysql_grouped)
  redshift_grouped = redshift_grouped.add_suffix('_tdw')
  mysql_grouped = mysql_grouped.add_suffix('_rds')
  redshift_grouped.rename(columns={'property_id_tdw':'property_id','month_tdw':'month'},inplace=True)
  mysql_grouped.rename(columns={'property_id_rds':'property_id','month_rds':'month'},inplace=True)
  
  if redshift_df.empty and mysql_df.empty:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','month'], how='outer', indicator=True)
    return comparison_df
  else:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','month'], how='outer', indicator=True)
    comparison_df['_merge'] = comparison_df['_merge'].map({'left_only': 'TDW', 'right_only': 'RDS', 'both': 'Both'})
    comparison_df['m'] = pd.to_datetime(comparison_df['month'],format= '%B-%y')
    comparison_df = comparison_df.sort_values(['property_id','m'],ascending=[True,False])
    comparison_df = comparison_df.drop(columns=['m'])
    columns_base=['total_tasks']
    comparison_df['Status'] = comparison_df.apply(
         lambda row: 'match' if all(row[f'{col}_rds'] == row[f'{col}_tdw'] for col in columns_base)
    else 'mismatch', axis=1
     )
    return comparison_df

def total_tasks_created_by_name(redshift_engine,mysql_engine):
  print("total_tasks_created_by_name query running")

  # adjusted_end_date,first_day_of_month = setDate_M_D_Y(tdw_date)

  redshift_query =f"""       
select property_id,contact ,count(*) as number_of_tasks from (
    select distinct tpd.src_property_id as property_id, q1.full_name as contact,src_task_id 
    from tdw.tdw_tasks_af q
    left join tdw.tdw_contact_d q1 on q.task_created_by_sk=q1.contact_sk
    left join tdw.tdw_property_d tpd on q.property_sk = tpd.property_sk
    where tpd.property_sk  in ({property_sk}) 
    ) as q
    group by 1,2
    order by 1,2;
          """
  
  mysql_query =f"""
  use hummingbird;

  SELECT property_id,case when  contact=' ' then 'Not Available'
  else contact 
  end as contact,count(*) as number_of_tasks from (
      select distinct p.id as property_id, concat(IFNULL(c.`first`, ''),' ',IFNULL(c.`last`, '')) as contact,
      t.id from hummingbird.tasks t
      inner JOIN hummingbird.task_types tt ON tt.id = t.task_type_id
    LEFT JOIN hummingbird.task_objects tko ON tko.task_id = t.id
    LEFT JOIN hummingbird.properties p ON p.id = tko.property_id
    left join hummingbird.contacts c on t.created_by  = c.id
      where p.id in ({property_id})) t
      group by 1,2
    order by 1,2;
      """

  redshift_df = fetch_data(redshift_engine, redshift_query)
  mysql_df = execute_mysql_statements(mysql_engine, mysql_query)

#   mysql_df.rename(columns={'move_in_month':'date'},inplace=True)
#   mysql_df['date'] = mysql_df['date'].apply(convert_to_last_day)

#   redshift_df.rename(columns={'calendar_month':'date'},inplace=True)
#   redshift_df['date'] = redshift_df['date'].apply(convert_to_last_day_by_numeric_month)

  redshift_grouped = redshift_df
  mysql_grouped = mysql_df
  
#  redshift_grouped['dateee'] = redshift_grouped['dateee'].str.replace(' ', '', regex=False)  

#   redshift_grouped['date'] = pd.to_datetime(redshift_grouped['date'])
#   mysql_grouped['date'] = pd.to_datetime(mysql_grouped['date'])


  # print(redshift_grouped,mysql_grouped)
  redshift_grouped = redshift_grouped.add_suffix('_tdw')
  mysql_grouped = mysql_grouped.add_suffix('_rds')
  redshift_grouped.rename(columns={'property_id_tdw':'property_id','contact_tdw':'contact'},inplace=True)
  mysql_grouped.rename(columns={'property_id_rds':'property_id','contact_rds':'contact'},inplace=True)
  
  if redshift_df.empty and mysql_df.empty:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','contact'], how='outer', indicator=True)
    return comparison_df
  else:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','contact'], how='outer', indicator=True)
    comparison_df['_merge'] = comparison_df['_merge'].map({'left_only': 'TDW', 'right_only': 'RDS', 'both': 'Both'})
    #comparison_df['mim'] = pd.to_datetime(comparison_df['contact'],format= '%B-%y')
    comparison_df = comparison_df.sort_values(['property_id','contact'])
    # comparison_df = comparison_df.drop(columns=['mim'])
    columns_base=['number_of_tasks']
    comparison_df['Status'] = comparison_df.apply(
         lambda row: 'match' if all(row[f'{col}_rds'] == row[f'{col}_tdw'] for col in columns_base)
    else 'mismatch', axis=1
     )
    return comparison_df

def total_tasks_completed(redshift_engine,mysql_engine):
  print("total_tasks_completed query running")

  # adjusted_end_date,first_day_of_month = setDate_M_D_Y(tdw_date)

  redshift_query =f"""       
   select property_id,contact,count(*) as tasks_completed from (
    select distinct tpdv.src_property_id as property_id,ttdv.full_name as contact,ta.src_task_id
    from tdw_ro.tdw_tasks_af_v ta
    left join tdw_ro.tdw_date_d_v dt ON ta.task_completed_date_sk=dt.date_sk
    left join tdw_ro.tdw_task_status_d_v st on st.task_status_sk=ta.task_status_sk
    left join tdw_ro.tdw_property_d_v tpdv on ta.property_sk = tpdv.property_sk
    left join tdw_ro.tdw_tenant_d_v ttdv on ta.task_created_by_sk = ttdv.tenant_sk
    left join tdw_ro.tdw_date_d_v tddv on ta.task_due_date_sk = tddv.date_sk
    where ta.property_sk in  ({property_sk}) and 
    dt.date_dt <= tddv.date_dt
    and lower(st.task_status_desc) = 'completed'
    order by src_task_id) q
    group by 1,2
    order by 1,2;
      """
  
  mysql_query =f"""
  use hummingbird;

  SELECT property_id,
  case when  contact=' ' then 'Not Available'
  else contact 
  end as contact

  ,count(*) as tasks_completed from (
      select distinct p.id as property_id, concat(IFNULL(c.`first`, ''),' ',IFNULL(c.`last`, '')) as contact,
      t.id 
      from hummingbird.tasks t
      inner JOIN hummingbird.task_types tt ON tt.id = t.task_type_id
    LEFT JOIN hummingbird.task_objects tko ON tko.task_id = t.id
    LEFT JOIN hummingbird.properties p ON p.id = tko.property_id
    left join hummingbird.contacts c on t.created_by = c.id
      where p.id in ({property_id}) and
      lower(t.status) = 'completed'
      and cast(DATE(CONVERT_TZ(t.status_updated_at , "+00:00", IFNULL(p.utc_offset, 'Etc/UTC'))) as date)  <= cast(DATE(CONVERT_TZ(t.due_date , "+00:00", IFNULL(p.utc_offset, 'Etc/UTC'))) as date)) t
      group by 1,2
      order by 1,2;
        """

  redshift_df = fetch_data(redshift_engine, redshift_query)
  mysql_df = execute_mysql_statements(mysql_engine, mysql_query)

#   mysql_df.rename(columns={'move_in_month':'date'},inplace=True)
#   mysql_df['date'] = mysql_df['date'].apply(convert_to_last_day)

#   redshift_df.rename(columns={'calendar_month':'date'},inplace=True)
#   redshift_df['date'] = redshift_df['date'].apply(convert_to_last_day_by_numeric_month)

  redshift_grouped = redshift_df
  mysql_grouped = mysql_df
  
#  redshift_grouped['dateee'] = redshift_grouped['dateee'].str.replace(' ', '', regex=False)  

#   redshift_grouped['date'] = pd.to_datetime(redshift_grouped['date'])
#   mysql_grouped['date'] = pd.to_datetime(mysql_grouped['date'])


  # print(redshift_grouped,mysql_grouped)
  redshift_grouped = redshift_grouped.add_suffix('_tdw')
  mysql_grouped = mysql_grouped.add_suffix('_rds')
  redshift_grouped.rename(columns={'property_id_tdw':'property_id','contact_tdw':'contact'},inplace=True)
  mysql_grouped.rename(columns={'property_id_rds':'property_id','contact_rds':'contact'},inplace=True)
  
  if redshift_df.empty and mysql_df.empty:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','contact'], how='outer', indicator=True)
    return comparison_df
  else:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','contact'], how='outer', indicator=True)
    comparison_df['_merge'] = comparison_df['_merge'].map({'left_only': 'TDW', 'right_only': 'RDS', 'both': 'Both'})
    #comparison_df['mim'] = pd.to_datetime(comparison_df['contact'],format= '%B-%y')
    comparison_df = comparison_df.sort_values(['property_id','contact'])
    # comparison_df = comparison_df.drop(columns=['mim'])
    columns_base=['tasks_completed']
    comparison_df['Status'] = comparison_df.apply(
         lambda row: 'match' if all(row[f'{col}_rds'] == row[f'{col}_tdw'] for col in columns_base)
    else 'mismatch', axis=1
     )
    return comparison_df

def total_tasks_per_task_type(redshift_engine,mysql_engine):
  print("total_tasks_completed query running")

  # adjusted_end_date,first_day_of_month = setDate_M_D_Y(tdw_date)

  redshift_query =f"""       
  
select  property_id,task_type,count(*) as task_type_count from (
    select distinct q.src_task_id , q1.task_type_desc as task_type, tpdv.src_property_id as property_id
    from tdw_ro.tdw_tasks_af_v q
    left join tdw_ro.tdw_task_type_d_v   q1 on q.task_type_sk=q1.task_type_sk
    left join tdw_ro.tdw_property_d_v tpdv on q.property_sk = tpdv.property_sk
    where tpdv.property_sk in ({property_sk})
    ) group by 1,2
    order by 1,2;
      """
  
  mysql_query =f"""
  use hummingbird;
 SELECT property_id,task_type,count(*) as task_type_count from (
    select distinct p.id as property_id,tt.label as task_type,
    t.id from hummingbird.tasks t
    inner JOIN task_types tt ON tt.id = t.task_type_id
	LEFT JOIN task_objects tko ON tko.task_id = t.id
	LEFT JOIN properties p ON p.id = tko.property_id
    where p.id in  ({property_id})) t
    group by 1,2
    order by 1,2
    ;

      """

  redshift_df = fetch_data(redshift_engine, redshift_query)
  mysql_df = execute_mysql_statements(mysql_engine, mysql_query)

#   mysql_df.rename(columns={'move_in_month':'date'},inplace=True)
#   mysql_df['date'] = mysql_df['date'].apply(convert_to_last_day)

#   redshift_df.rename(columns={'calendar_month':'date'},inplace=True)
#   redshift_df['date'] = redshift_df['date'].apply(convert_to_last_day_by_numeric_month)

  redshift_grouped = redshift_df
  mysql_grouped = mysql_df
  
#  redshift_grouped['dateee'] = redshift_grouped['dateee'].str.replace(' ', '', regex=False)  

#   redshift_grouped['date'] = pd.to_datetime(redshift_grouped['date'])
#   mysql_grouped['date'] = pd.to_datetime(mysql_grouped['date'])


  # print(redshift_grouped,mysql_grouped)
  redshift_grouped = redshift_grouped.add_suffix('_tdw')
  mysql_grouped = mysql_grouped.add_suffix('_rds')
  redshift_grouped.rename(columns={'property_id_tdw':'property_id','task_type_tdw':'task_type'},inplace=True)
  mysql_grouped.rename(columns={'property_id_rds':'property_id','task_type_rds':'task_type'},inplace=True)
  
  if redshift_df.empty and mysql_df.empty:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','task_type'], how='outer', indicator=True)
    return comparison_df
  else:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','task_type'], how='outer', indicator=True)
    comparison_df['_merge'] = comparison_df['_merge'].map({'left_only': 'TDW', 'right_only': 'RDS', 'both': 'Both'})
    #comparison_df['mim'] = pd.to_datetime(comparison_df['contact'],format= '%B-%y')
    comparison_df = comparison_df.sort_values(['property_id','task_type'])
    # comparison_df = comparison_df.drop(columns=['mim'])
    columns_base=['task_type_count']
    comparison_df['Status'] = comparison_df.apply(
         lambda row: 'match' if all(row[f'{col}_rds'] == row[f'{col}_tdw'] for col in columns_base)
    else 'mismatch', axis=1
     )
    return comparison_df


def total_completed_tasks_on_time(redshift_engine,mysql_engine):
  print("total_completed_tasks_on_time query running")

  # adjusted_end_date,first_day_of_month = setDate_M_D_Y(tdw_date)

  redshift_query =f"""       
  select property_id,count(*) as completed_tasks from (
    select  distinct t.src_task_id, tpdv.src_property_id as property_id
    from tdw_ro.tdw_tasks_af_v  t
    left join tdw_ro.tdw_date_d_v dt on t.task_due_date_sk = dt.date_sk
    left join tdw_ro.tdw_time_d_v ti on t.task_due_time_sk = ti.time_sk
    left join tdw_ro.tdw_task_status_d_v ss on t.task_status_sk = ss.task_status_sk
    left join tdw_ro.tdw_date_d_v dt1 on t.task_completed_date_sk = dt1.date_sk
    left join tdw_ro.tdw_time_d_v ti1 on t.task_completed_time_sk = ti1.time_sk
    left join tdw_ro.tdw_property_d_v tpdv on t.property_sk = tpdv.property_sk 
    where  lower(ss.task_status_desc) = 'completed'
    and tpdv.property_sk  in ({property_sk})
    and (dt.date_dt || ' ' || ti.time_stamp)::TIMESTAMP  >=   (dt1.date_dt || ' ' || ti1.time_stamp)::TIMESTAMP 
    ) as q
    group by 1;
      """
  
  mysql_query =f"""
      use hummingbird;
    SELECT property_id,count(*) as completed_tasks from (
        select distinct p.id as property_id,
        t.id from hummingbird.tasks t
        inner JOIN hummingbird.task_types tt ON tt.id = t.task_type_id
      LEFT JOIN hummingbird.task_objects tko ON tko.task_id = t.id
      LEFT JOIN hummingbird.properties p ON p.id = tko.property_id
        where p.id in ({property_id}) and
        lower(t.status) = 'completed'
        and t.due_date >= t.status_updated_at) t
        group by 1;
      """

  redshift_df = fetch_data(redshift_engine, redshift_query)
  mysql_df = execute_mysql_statements(mysql_engine, mysql_query)

#   mysql_df.rename(columns={'move_in_month':'date'},inplace=True)
#   mysql_df['date'] = mysql_df['date'].apply(convert_to_last_day)

#   redshift_df.rename(columns={'calendar_month':'date'},inplace=True)
#   redshift_df['date'] = redshift_df['date'].apply(convert_to_last_day_by_numeric_month)

  redshift_grouped = redshift_df
  mysql_grouped = mysql_df
  
#  redshift_grouped['dateee'] = redshift_grouped['dateee'].str.replace(' ', '', regex=False)  

#   redshift_grouped['date'] = pd.to_datetime(redshift_grouped['date'])
#   mysql_grouped['date'] = pd.to_datetime(mysql_grouped['date'])


  # print(redshift_grouped,mysql_grouped)
  redshift_grouped = redshift_grouped.add_suffix('_tdw')
  mysql_grouped = mysql_grouped.add_suffix('_rds')
  redshift_grouped.rename(columns={'property_id_tdw':'property_id'},inplace=True)
  mysql_grouped.rename(columns={'property_id_rds':'property_id'},inplace=True)
  
  if redshift_df.empty and mysql_df.empty:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id'], how='outer', indicator=True)
    return comparison_df
  else:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id'], how='outer', indicator=True)
    comparison_df['_merge'] = comparison_df['_merge'].map({'left_only': 'TDW', 'right_only': 'RDS', 'both': 'Both'})
    #comparison_df['mim'] = pd.to_datetime(comparison_df['contact'],format= '%B-%y')
    comparison_df = comparison_df.sort_values(['property_id'])
    # comparison_df = comparison_df.drop(columns=['mim'])
    columns_base=['completed_tasks']
    comparison_df['Status'] = comparison_df.apply(
         lambda row: 'match' if all(row[f'{col}_rds'] == row[f'{col}_tdw'] for col in columns_base)
    else 'mismatch', axis=1
     )
    return comparison_df


def total_pending_tasks(redshift_engine,mysql_engine):
  print("total_tasks_completed query running")

  # adjusted_end_date,first_day_of_month = setDate_M_D_Y(tdw_date)

  redshift_query =f"""  
  select property_id,contact, count(*) as pending_tasks from 
      (
      select distinct tpdv.src_property_id as property_id,ttdv.full_name as contact,t.src_task_id
      from tdw_ro.tdw_tasks_af_v t
      left join tdw_ro.tdw_date_d_v dt on t.task_due_date_sk=dt.date_sk
      left join tdw_ro.tdw_task_status_d_v ss on t.task_status_sk=ss.task_status_sk
      left join tdw_ro.tdw_property_d_v tpdv on t.property_sk = tpdv.property_sk
      left join tdw_ro.tdw_tenant_d_v ttdv on t.task_assigned_to_sk = ttdv.tenant_sk
      where dt.date_dt < current_date
      and tpdv.property_sk  in ({property_sk})
      and lower(ss.task_status_desc) = 'pending'
      order by t.src_task_id
      )  as q
      group by 1,2
      order by 1,2 ;
      """
  
  mysql_query =f"""
  use hummingbird;

  SELECT property_id,
  case when  contact=' ' then 'Not Available'
       else contact 
       end as contact,
  count(*) as pending_tasks from (
      select distinct p.id as property_id, concat(IFNULL(c.`first`, ''),' ',IFNULL(c.`last`, '')) as contact,
      t.id from hummingbird.tasks t
      inner JOIN hummingbird.task_types tt ON tt.id = t.task_type_id
    LEFT JOIN hummingbird.task_objects tko ON tko.task_id = t.id
    LEFT JOIN hummingbird.properties p ON p.id = tko.property_id
      left join hummingbird.task_assignees ta on t.id = ta.task_id 
    left join hummingbird.contacts c on ta.assignee_id  = c.id
      where p.id in  ({property_id}) and
      lower(t.status) = 'pending'
      and DATE(CONVERT_TZ(t.due_date , "+00:00", IFNULL(p.utc_offset, 'Etc/UTC'))) < current_date()) t
      group by 1,2
    order by 1,2
      ;
      """

  redshift_df = fetch_data(redshift_engine, redshift_query)
  mysql_df = execute_mysql_statements(mysql_engine, mysql_query)

#   mysql_df.rename(columns={'move_in_month':'date'},inplace=True)
#   mysql_df['date'] = mysql_df['date'].apply(convert_to_last_day)

#   redshift_df.rename(columns={'calendar_month':'date'},inplace=True)
#   redshift_df['date'] = redshift_df['date'].apply(convert_to_last_day_by_numeric_month)

  redshift_grouped = redshift_df
  mysql_grouped = mysql_df
  
#  redshift_grouped['dateee'] = redshift_grouped['dateee'].str.replace(' ', '', regex=False)  

#   redshift_grouped['date'] = pd.to_datetime(redshift_grouped['date'])
#   mysql_grouped['date'] = pd.to_datetime(mysql_grouped['date'])


  # print(redshift_grouped,mysql_grouped)
  redshift_grouped = redshift_grouped.add_suffix('_tdw')
  mysql_grouped = mysql_grouped.add_suffix('_rds')
  redshift_grouped.rename(columns={'property_id_tdw':'property_id','contact_tdw':'contact'},inplace=True)
  mysql_grouped.rename(columns={'property_id_rds':'property_id','contact_rds':'contact'},inplace=True)
  
  if redshift_df.empty and mysql_df.empty:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','contact'], how='outer', indicator=True)
    return comparison_df
  else:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','contact'], how='outer', indicator=True)
    comparison_df['_merge'] = comparison_df['_merge'].map({'left_only': 'TDW', 'right_only': 'RDS', 'both': 'Both'})
    #comparison_df['mim'] = pd.to_datetime(comparison_df['contact'],format= '%B-%y')
    comparison_df = comparison_df.sort_values(['property_id','contact'],ascending=[True,False])
    # comparison_df = comparison_df.drop(columns=['mim'])
    columns_base=['pending_tasks']
    comparison_df['Status'] = comparison_df.apply(
         lambda row: 'match' if all(row[f'{col}_rds'] == row[f'{col}_tdw'] for col in columns_base)
    else 'mismatch', axis=1
     )
    return comparison_df
 
def leads_activity(redshift_engine,mysql_engine,tdw_date,rds_date):
  print("leads_activity query running")

  adjusted_end_date,first_day_of_month = setDate_M_D_Y(tdw_date)

  redshift_query =f"""       
    SELECT
  src_property_id AS property_id,
  TO_CHAR(DATE_TRUNC('month', date_dt), 'Month-YY') AS month,
  COUNT(src_tenant_id) AS lead_count,
  category_source
FROM (
    SELECT
        pp.src_property_id,
        l.src_lead_id,
        ls.lead_source_desc,
        li.lead_interaction_desc,
        LOWER(s.lead_status_desc),
        CASE
            WHEN ls.lead_source_desc IN ('XPS Solutions','XPS Application','XPS','SMS','Sign call','Phone Call','DR / Phone','DR','CallPotential Application','CallCenter','Call Potenial','Call in','Call', 'Call Center')
              OR li.lead_interaction_desc IN ('Phone', 'Charm Phone Call')
            THEN 'Phone Leads'
            WHEN ls.lead_source_desc IN ('Walk-In - AE','Walk-In','Walk In','store','Referral','In-store','Facility','Drive-by','Drive By')
              OR li.lead_interaction_desc IN ('Walk In', 'Walk-In')
            THEN 'Walk-In Leads'
            WHEN ls.lead_source_desc IN ('Website Application', 'Website API Access', 'Website','Tenant Website V2', 'Tenant Website', 'Tenant V2 Website','Tenant V2', 'Storelocal Website Application', 'Storelocal Website','StoreLocal', 'Store Local', 'StoragePug Website Reservation','StoragePug Website Lead','Sparefoot','sparefoot','Social Media','pms lead','Online Search','Nectar','Mariposa Website','JAC Website','Internet','Google Search','Google','Email','Company Website', 'Storagefront', 'Storelocal', 'Mariposa Website Application')
            THEN 'Web Leads'
            ELSE 'Others'
        END AS category_source,
        src_tenant_id,
        dt.date_dt
    FROM tdw_ro.tdw_leads_tf_v l
    LEFT JOIN tdw_ro.tdw_lead_source_d_v ls ON l.lead_source_sk = ls.lead_source_sk
    LEFT JOIN tdw_ro.tdw_tenant_d_v c ON l.tenant_sk = c.tenant_sk
    LEFT JOIN tdw_ro.tdw_lead_status_d_v s ON s.lead_status_sk = l.lead_status_sk
    LEFT JOIN tdw_ro.tdw_date_d_v dt ON l.generated_on_date_sk = dt.date_sk
    LEFT JOIN tdw_ro.tdw_leads_interaction_d_v li ON li.lead_interaction_sk = l.lead_interaction_sk
    LEFT JOIN tdw_ro.tdw_property_d_v pp ON pp.property_sk = l.property_sk
    WHERE
        date_dt BETWEEN DATE '{first_day_of_month}' and '{adjusted_end_date}'
        AND l.property_sk in ({property_sk})
)
GROUP BY
  src_property_id,
  TO_CHAR(DATE_TRUNC('month', date_dt), 'Month-YY'),
  category_source
ORDER BY
src_property_id,
  TO_CHAR(DATE_TRUNC('month', date_dt), 'Month-YY'),
  category_source;
      """
  
  mysql_query =f"""
  use hummingbird;
  set @date = '{rds_date}';
  
  select property_id,DATE_FORMAT(local_created_at, '%M-%y') as month ,count(*) as lead_count, category_source   from
(
select * from
(SELECT distinct ld.local_created_at, ld.id,ld.property_id,
concat(if(isnull(p.number), '', concat(p.number, ' - ')), p.name) as property_name,
c.id as contact_id, TRIM(CONCAT(IFNULL(c.first, ''), ' ', IFNULL(c.last, ''))) as name
-- , cp.phone
, c.email, u.number as unit_number,
ifnull(ld.move_in_date,l.start_date) as move_in_date, ld.status, ld.source,
CASE
when ld.source in ('XPS Solutions','XPS Application','XPS','SMS','Sign call','Phone Call','DR / Phone','DR','CallPotential Application','CallCenter','Call Potenial','Call in','Call', 'Call Center') or ld.interaction_initiated IN ('Phone', 'Charm Phone Call') then 'Phone Leads'
when ld.source in ('Walk-In - AE','Walk-In','Walk In','store','Referral','In-store','Facility','Drive-by','Drive By') or ld.interaction_initiated IN ('Walk In', 'Walk-In') then 'Walk-In Leads'
when ld.source in ('Website Application', 'Website API Access', 'Website','Tenant Website V2', 'Tenant Website', 'Tenant V2 Website','Tenant V2', 'Storelocal Website Application', 'Storelocal Website','StoreLocal', 'Store Local', 'StoragePug Website Reservation','StoragePug Website Lead','Sparefoot','sparefoot','Social Media','pms lead','Online Search','Nectar','Mariposa Website','JAC Website','Internet','Google Search','Google','Email','Company Website', 'Storagefront', 'Storelocal', 'Mariposa Website Application') then 'Web Leads'
else 'Others'
end as category_source,
DATE(ld.local_created_at) as lead_date
FROM leads ld
join contacts c on ld.contact_id = c.id
left join contact_phones cp on c.id = cp.contact_id and cp.primary = 1
left join units u on u.id = ld.unit_id
left join properties p on p.id = ld.property_id
left join leases l on l.id = ld.lease_id
WHERE
ld.property_id   in  ({property_id})
and DATE(ld.local_created_at) between   DATE_FORMAT(DATE_SUB(@date, INTERVAL 1 YEAR), '%Y-%m-01') and @date
) as q
-- where category_source='Others'
order by id
) as q1 group by property_id,DATE_FORMAT(local_created_at, '%M-%y'),category_source
order by property_id,DATE_FORMAT(local_created_at, '%M-%y'),category_source;
     """

  redshift_df = fetch_data(redshift_engine, redshift_query)
  mysql_df = execute_mysql_statements(mysql_engine, mysql_query)

#   mysql_df.rename(columns={'move_in_month':'date'},inplace=True)
#   mysql_df['date'] = mysql_df['date'].apply(convert_to_last_day)

#   redshift_df.rename(columns={'calendar_month':'date'},inplace=True)
#   redshift_df['date'] = redshift_df['date'].apply(convert_to_last_day_by_numeric_month)

  redshift_grouped = redshift_df
  mysql_grouped = mysql_df
  
  redshift_grouped['month'] = redshift_grouped['month'].str.replace(' ', '', regex=False)  

#   redshift_grouped['date'] = pd.to_datetime(redshift_grouped['date'])
#   mysql_grouped['date'] = pd.to_datetime(mysql_grouped['date'])


  # print(redshift_grouped,mysql_grouped)
  redshift_grouped = redshift_grouped.add_suffix('_tdw')
  mysql_grouped = mysql_grouped.add_suffix('_rds')
  redshift_grouped.rename(columns={'property_id_tdw':'property_id','month_tdw':'month','category_source_tdw':'category_source'},inplace=True)
  mysql_grouped.rename(columns={'property_id_rds':'property_id','month_rds':'month','category_source_rds':'category_source'},inplace=True)
  
  if redshift_df.empty and mysql_df.empty:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','month','category_source'], how='outer', indicator=True)
    return comparison_df
  else:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','month','category_source'], how='outer', indicator=True)
    comparison_df['_merge'] = comparison_df['_merge'].map({'left_only': 'TDW', 'right_only': 'RDS', 'both': 'Both'})
    comparison_df['m'] = pd.to_datetime(comparison_df['month'],format= '%B-%y')
    comparison_df = comparison_df.sort_values(['property_id','m'],ascending=[True,False])
    comparison_df = comparison_df.drop(columns=['m'])
    columns_base=['lead_count']
    comparison_df['Status'] = comparison_df.apply(
         lambda row: 'match' if all(row[f'{col}_rds'] == row[f'{col}_tdw'] for col in columns_base)
    else 'mismatch', axis=1
     )
    return comparison_df

 
def invoices(redshift_engine,mysql_engine,tdw_date,rds_date):
  print("invoices query running")

  adjusted_end_date,first_day_of_month = setDate_M_D_Y(tdw_date)

  redshift_query =f"""       
      with qwe  as (
select distinct pp.src_property_id as property_id,ilt.src_invoice_line_id , ( nvl(ilt.quantity,0) * nvl(ilt.rate,0) ) as subtotal,nvl(ilt.tax,0) as tax_amount  ,  nvl(ilt.discount,0) as discount_amount, 
( nvl(ilt.quantity,0) * nvl(ilt.rate,0) )  -    nvl(ilt.discount,0) as amount ,
 case when pd.product_type_cd = 'rent' then 'rent'
           when lower(pd.product_type_cd) = 'insurance' then 'insurance'
           when lower(pd.product_type_cd) = 'late' then 'fee'
           when lower(pd.product_type_cd) = 'product' then 'merchandise'
           else 'others'
         end as product,
         d1.date_dt

from tdw_ro.tdw_invoice_line_tf_v     ilt
inner join tdw_ro.tdw_product_d_v     pd
on ilt.product_sk=pd.product_sk
inner join  tdw_ro.tdw_date_d_v  d 
on d.date_sk= ilt.invoice_voided_date_sk
inner join   tdw_ro.tdw_date_d_v  d1
on ilt.invoice_due_date_sk=d1.date_sk
inner join tdw_ro.tdw_property_d_v pp
on pp.property_sk=ilt.property_sk

--inner join  tdw.tdw_date_d  d2
--on ilt.invoice_date_sk=d2.date_sk
where lower(pd.product_type_cd) not in  ('auction', 'security', 'cleaning')
and ilt.property_sk in ({property_sk})
and (d.date_dt is null or d.date_dt >CURRENT_DATE)
and d1.date_dt between '{first_day_of_month}' and '{adjusted_end_date}'

)


 select 
         cil.property_id,
        to_char(date_dt,'Month-YY') as month,
         sum(case when cil.product = 'rent' then cil.subtotal else 0 end) as rent,
         sum(case when cil.product = 'fee'  then cil.subtotal else 0 end) as fee,
         sum(case when cil.product = 'insurance' then cil.subtotal else 0 end) as insurance,
         sum(case when cil.product = 'merchandise' then cil.subtotal else 0 end) as merchandise,
         sum(case when cil.product = 'others' then cil.subtotal else 0 end) as others,
         sum(cil.tax_amount) as tax,
         sum(cil.subtotal) as subtotal,
         sum(cil.discount_amount) as discounts
       from qwe cil
      group by cil.property_id,to_char(date_dt,'Month-YY')
      order by  cil.property_id,to_char(date_dt,'Month-YY');
      """
  
  mysql_query =f"""
  use hummingbird;
  set @date = '{rds_date}';
  with cte_invoice_lines as ( 
       select i.property_id,
         concat(if(isnull(pr.number), '', concat(pr.number, ' - ')), pr.name) as property_name,
         concat(c.first,' ',c.last) as name, i.id as invoice_id, i.date as invoice_created_at, i.due as invoice_date, i.number as invoice_number, u.number as unit_number, p.name as product_name,
         (ifnull(il.qty, 0) * ifnull(il.cost, 0)) as subtotal, ifnull(il.total_tax, 0) as tax_amount, ifnull(il.total_discounts, 0) as discount_amount,
         (ifnull(il.qty, 0) * ifnull(il.cost, 0)) - ifnull(il.total_discounts, 0) as amount, p.default_type as default_type,
         case when p.default_type = 'rent' then 'rent'
           when p.default_type = 'insurance' then 'insurance'
           when p.default_type = 'late' then 'fee'
           when p.default_type = 'product' then 'merchandise'
           else 'others'
         end as product
         ,(select label from unit_types where id = u.unit_type_id) as unit_type,(select name from unit_types where id = u.unit_type_id) as unit_type_name,p.id as product_id,
         ifnull(ppr.income_account_id,p.income_account_id) as income_account_id,
         ifnull(p.slug,p.default_type) as product_slug
       from invoices i
         join invoice_lines il on il.invoice_id = i.id
         join products p on p.id = il.product_id
         left join leases l on l.id = i.lease_id
         left join contacts c on c.id = i.contact_id
         left join units u on u.id = l.unit_id
         left join properties pr on pr.id = u.property_id
         left join property_products ppr on ppr.product_id = p.id and ppr.property_id = i.property_id 
       where p.default_type not in ('auction', 'security', 'cleaning')
         and i.property_id in  ({property_id})
         and (i.void_date is null or i.void_date > current_date)
         and i.due between    DATE_FORMAT(DATE_SUB(@date, INTERVAL 1 YEAR), '%Y-%m-01') and @date
      order by i.due 
       )
       SELECT
         cil.property_id,
         DATE_FORMAT(cil.invoice_date, '%M-%y') as month,
         sum(case when cil.product = 'rent' then cil.subtotal else 0 end) as rent,
         sum(case when cil.product = 'fee'  then cil.subtotal else 0 end) as fee,
         sum(case when cil.product = 'insurance' then cil.subtotal else 0 end) as insurance,
         sum(case when cil.product = 'merchandise' then cil.subtotal else 0 end) as merchandise,
         sum(case when cil.product = 'others' then cil.subtotal else 0 end) as others,
         sum(cil.tax_amount) as tax,
         sum(cil.subtotal) as subtotal,
         sum(cil.discount_amount) as discounts
       from cte_invoice_lines cil
       group by property_id, DATE_FORMAT(cil.invoice_date, '%M-%y')
	   
 	    order by property_id, DATE_FORMAT(cil.invoice_date, '%M-%y');
     """

  redshift_df = fetch_data(redshift_engine, redshift_query)
  mysql_df = execute_mysql_statements(mysql_engine, mysql_query)

#   mysql_df.rename(columns={'move_in_month':'date'},inplace=True)
#   mysql_df['date'] = mysql_df['date'].apply(convert_to_last_day)

#   redshift_df.rename(columns={'calendar_month':'date'},inplace=True)
#   redshift_df['date'] = redshift_df['date'].apply(convert_to_last_day_by_numeric_month)

  redshift_grouped = redshift_df
  mysql_grouped = mysql_df
  
  redshift_grouped['month'] = redshift_grouped['month'].str.replace(' ', '', regex=False)  

#   redshift_grouped['date'] = pd.to_datetime(redshift_grouped['date'])
#   mysql_grouped['date'] = pd.to_datetime(mysql_grouped['date'])


  # print(redshift_grouped,mysql_grouped)
  redshift_grouped = redshift_grouped.add_suffix('_tdw')
  mysql_grouped = mysql_grouped.add_suffix('_rds')
  redshift_grouped.rename(columns={'property_id_tdw':'property_id','month_tdw':'month'},inplace=True)
  mysql_grouped.rename(columns={'property_id_rds':'property_id','month_rds':'month'},inplace=True)
  
  if redshift_df.empty and mysql_df.empty:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','month'], how='outer', indicator=True)
    return comparison_df
  else:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','month'], how='outer', indicator=True)
    comparison_df['_merge'] = comparison_df['_merge'].map({'left_only': 'TDW', 'right_only': 'RDS', 'both': 'Both'})
    comparison_df['m'] = pd.to_datetime(comparison_df['month'],format= '%B-%y')
    comparison_df = comparison_df.sort_values(['property_id','m'],ascending=[True,False])
    comparison_df = comparison_df.drop(columns=['m'])
    columns_base=['rent','fee','insurance','merchandise','others','tax','subtotal','discounts']
    comparison_df['Status'] = comparison_df.apply(
         lambda row: 'match' if all(row[f'{col}_rds'] == row[f'{col}_tdw'] for col in columns_base)
    else 'mismatch', axis=1
     )
    return comparison_df
  

def Occupied_Units(redshift_engine,mysql_engine,tdw_date,rds_date):
  print("Occupied query running")
  
  date_conditions=setDate_For_Occupancy(tdw_date)

  redshift_query = f"""
  SELECT tpdv.src_property_id as property_id,to_char(dd.date_dt,'Month-YY') as occupied_month, COUNT(dd.date_dt) as occupied_count,
  sum(space_area) as occupied_space_area
  FROM tdw_ro.tdw_occupancy_by_space_pf_v spv 
  JOIN tdw_ro.tdw_date_d_v dd ON spv.date_sk = dd.date_sk
  join tdw_ro.tdw_property_d_v tpdv on spv.property_sk = tpdv.property_sk
  WHERE
  spv.property_sk in ({property_sk}) AND
  ({date_conditions}) AND 
  spv.occupied_space_count = 1
  GROUP BY tpdv.src_property_id,to_char(dd.date_dt,'Month-YY')
  ORDER BY property_id,occupied_month;
  """

  mysql_query = f"""
use hummingbird;
set @date = '{rds_date}';

-- Occupied and Complimentary	

    WITH occupied_units as (
    select
      u.property_id,
      concat(if(isnull(p.number), '', concat(p.number, ' - ')), p.name) as property_name,
      u.status,
      u.id as unit_id,
      l.id as lease_id,
      u.category_id as unit_category_id,
      (
      select
        label
      from
        unit_types
      where
        id = u.unit_type_id) as unit_type,
      (
      select
        name
      from
        unit_types
      where
        id = u.unit_type_id) as unit_type_name,
      upc.id as unit_price_id,
      (cast(
            (
              select (
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
                * 
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
              )
              from units un where un.id = u.id
            )
          as decimal(10, 2)))
          as occupied_sqft,
      ifnull(upc.price, 0) as occupied_base_rent,
      ifnull(upc.set_rate, 0) as occupied_set_rate,
      ifnull(s.price, 0) as occupied_rent,
      TRIM(CONCAT(IFNULL(c.first, ''), ' ', IFNULL(c.last, ''))) as tenant_name,
      u.number,
      u.label,
      uc.description,
      LAST_DAY(@date) as date
    from
      units u
    join properties p on
      p.id = u.property_id
    join leases l on
      l.unit_id = u.id
    left join services s on
      s.lease_id = l.id
      and s.product_id = u.product_id
      and s.status = 1
      and (LAST_DAY(@date) >= s.start_date
        and (s.end_date is null
          or s.end_date >= LAST_DAY(@date)))
    left join (
      select
        upc.*
      from
        unit_price_changes upc
      join (
        select
          MAX(upc.id) as max_id
        from
          unit_price_changes upc
        join units u on
          u.id = upc.unit_id
        join properties p on
          p.id = u.property_id
        where
          DATE(CONVERT_TZ(upc.start, '+00:00', p.utc_offset )) <= LAST_DAY(@date)
            and (upc.end is null
              or DATE(CONVERT_TZ(upc.end, '+00:00', p.utc_offset )) > LAST_DAY(@date))
              and u.property_id in ({property_id})
            group by
              upc.unit_id
              ) tupc on
        tupc.max_id = upc.id
            ) upc on
      upc.unit_id = u.id
    left join contact_leases cl on
      cl.lease_id = l.id
      and `primary` = 1
    left join contacts c on
      c.id = cl.contact_id
    left join unit_categories uc on
      uc.id = u.category_id
    where
      u.property_id in ({property_id})
      and l.status = 1
      and (LAST_DAY(@date) >= l.start_date
        and (l.end_date is null
          or l.end_date > LAST_DAY(@date)))
    group by
      u.id
    UNION ALL
    select
      u.property_id,
      concat(if(isnull(p.number), '', concat(p.number, ' - ')), p.name) as property_name,
      u.status,
      u.id as unit_id,
      l.id as lease_id,
      u.category_id as unit_category_id,
      (
      select
        label
      from
        unit_types
      where
        id = u.unit_type_id) as unit_type,
      (
      select
        name
      from
        unit_types
      where
        id = u.unit_type_id) as unit_type_name,
      upc.id as unit_price_id,
      (cast(
            (
              select (
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
                * 
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
              )
              from units un where un.id = u.id
            )
          as decimal(10, 2)))
          as occupied_sqft,
      ifnull(upc.price, 0) as occupied_base_rent,
      ifnull(upc.set_rate, 0) as occupied_set_rate,
      ifnull(s.price, 0) as occupied_rent,
      TRIM(CONCAT(IFNULL(c.first, ''), ' ', IFNULL(c.last, ''))) as tenant_name,
      u.number,
      u.label,
      uc.description,
      LAST_DAY(DATE_SUB(@date, INTERVAL 1 MONTH)) as date
    from
      units u
    join properties p on
      p.id = u.property_id
    join leases l on
      l.unit_id = u.id
    left join services s on
      s.lease_id = l.id
      and s.product_id = u.product_id
      and s.status = 1
      and (LAST_DAY(DATE_SUB(@date, INTERVAL 1 MONTH)) >= s.start_date
        and (s.end_date is null
          or s.end_date >= LAST_DAY(DATE_SUB(@date, INTERVAL 1 MONTH))))
    left join (
      select
        upc.*
      from
        unit_price_changes upc
      join (
        select
          MAX(upc.id) as max_id
        from
          unit_price_changes upc
        join units u on
          u.id = upc.unit_id
        join properties p on
          p.id = u.property_id
        where
          DATE(CONVERT_TZ(upc.start, '+00:00', p.utc_offset )) <= LAST_DAY(DATE_SUB(@date, INTERVAL 1 MONTH))
            and (upc.end is null
              or DATE(CONVERT_TZ(upc.end, '+00:00', p.utc_offset )) > LAST_DAY(DATE_SUB(@date, INTERVAL 1 MONTH)))
              and u.property_id in ({property_id})
            group by
              upc.unit_id
              ) tupc on
        tupc.max_id = upc.id
            ) upc on
      upc.unit_id = u.id
    left join contact_leases cl on
      cl.lease_id = l.id
      and `primary` = 1
    left join contacts c on
      c.id = cl.contact_id
    left join unit_categories uc on
      uc.id = u.category_id
    where
      u.property_id in ({property_id})
      and l.status = 1
      and (LAST_DAY(DATE_SUB(@date, INTERVAL 1 MONTH)) >= l.start_date
        and (l.end_date is null
          or l.end_date > LAST_DAY(DATE_SUB(@date, INTERVAL 1 MONTH))))
    group by
      u.id
    UNION ALL
    select
      u.property_id,
      concat(if(isnull(p.number), '', concat(p.number, ' - ')), p.name) as property_name,
      u.status,
      u.id as unit_id,
      l.id as lease_id,
      u.category_id as unit_category_id,
      (
      select
        label
      from
        unit_types
      where
        id = u.unit_type_id) as unit_type,
      (
      select
        name
      from
        unit_types
      where
        id = u.unit_type_id) as unit_type_name,
      upc.id as unit_price_id,
      (cast(
            (
              select (
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
                * 
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
              )
              from units un where un.id = u.id
            )
          as decimal(10, 2)))
          as occupied_sqft,
      ifnull(upc.price, 0) as occupied_base_rent,
      ifnull(upc.set_rate, 0) as occupied_set_rate,
      ifnull(s.price, 0) as occupied_rent,
      TRIM(CONCAT(IFNULL(c.first, ''), ' ', IFNULL(c.last, ''))) as tenant_name,
      u.number,
      u.label,
      uc.description,
      LAST_DAY(DATE_SUB(@date, INTERVAL 2 MONTH)) as date
    from
      units u
    join properties p on
      p.id = u.property_id
    join leases l on
      l.unit_id = u.id
    left join services s on
      s.lease_id = l.id
      and s.product_id = u.product_id
      and s.status = 1
      and (LAST_DAY(DATE_SUB(@date, INTERVAL 2 MONTH)) >= s.start_date
        and (s.end_date is null
          or s.end_date >= LAST_DAY(DATE_SUB(@date, INTERVAL 2 MONTH))))
    left join (
      select
        upc.*
      from
        unit_price_changes upc
      join (
        select
          MAX(upc.id) as max_id
        from
          unit_price_changes upc
        join units u on
          u.id = upc.unit_id
        join properties p on
          p.id = u.property_id
        where
          DATE(CONVERT_TZ(upc.start, '+00:00', p.utc_offset )) <= LAST_DAY(DATE_SUB(@date, INTERVAL 2 MONTH))
            and (upc.end is null
              or DATE(CONVERT_TZ(upc.end, '+00:00', p.utc_offset )) > LAST_DAY(DATE_SUB(@date, INTERVAL 2 MONTH)))
              and u.property_id in ({property_id})
            group by
              upc.unit_id
              ) tupc on
        tupc.max_id = upc.id
            ) upc on
      upc.unit_id = u.id
    left join contact_leases cl on
      cl.lease_id = l.id
      and `primary` = 1
    left join contacts c on
      c.id = cl.contact_id
    left join unit_categories uc on
      uc.id = u.category_id
    where
      u.property_id in ({property_id})
      and l.status = 1
      and (LAST_DAY(DATE_SUB(@date, INTERVAL 2 MONTH)) >= l.start_date
        and (l.end_date is null
          or l.end_date > LAST_DAY(DATE_SUB(@date, INTERVAL 2 MONTH))))
    group by
      u.id
    UNION ALL
    select
      u.property_id,
      concat(if(isnull(p.number), '', concat(p.number, ' - ')), p.name) as property_name,
      u.status,
      u.id as unit_id,
      l.id as lease_id,
      u.category_id as unit_category_id,
      (
      select
        label
      from
        unit_types
      where
        id = u.unit_type_id) as unit_type,
      (
      select
        name
      from
        unit_types
      where
        id = u.unit_type_id) as unit_type_name,
      upc.id as unit_price_id,
      (cast(
            (
              select (
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
                * 
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
              )
              from units un where un.id = u.id
            )
          as decimal(10, 2)))
          as occupied_sqft,
      ifnull(upc.price, 0) as occupied_base_rent,
      ifnull(upc.set_rate, 0) as occupied_set_rate,
      ifnull(s.price, 0) as occupied_rent,
      TRIM(CONCAT(IFNULL(c.first, ''), ' ', IFNULL(c.last, ''))) as tenant_name,
      u.number,
      u.label,
      uc.description,
      LAST_DAY(DATE_SUB(@date, INTERVAL 3 MONTH)) as date
    from
      units u
    join properties p on
      p.id = u.property_id
    join leases l on
      l.unit_id = u.id
    left join services s on
      s.lease_id = l.id
      and s.product_id = u.product_id
      and s.status = 1
      and (LAST_DAY(DATE_SUB(@date, INTERVAL 3 MONTH)) >= s.start_date
        and (s.end_date is null
          or s.end_date >= LAST_DAY(DATE_SUB(@date, INTERVAL 3 MONTH))))
    left join (
      select
        upc.*
      from
        unit_price_changes upc
      join (
        select
          MAX(upc.id) as max_id
        from
          unit_price_changes upc
        join units u on
          u.id = upc.unit_id
        join properties p on
          p.id = u.property_id
        where
          DATE(CONVERT_TZ(upc.start, '+00:00', p.utc_offset )) <= LAST_DAY(DATE_SUB(@date, INTERVAL 3 MONTH))
            and (upc.end is null
              or DATE(CONVERT_TZ(upc.end, '+00:00', p.utc_offset )) > LAST_DAY(DATE_SUB(@date, INTERVAL 3 MONTH)))
              and u.property_id in ({property_id})
            group by
              upc.unit_id
              ) tupc on
        tupc.max_id = upc.id
            ) upc on
      upc.unit_id = u.id
    left join contact_leases cl on
      cl.lease_id = l.id
      and `primary` = 1
    left join contacts c on
      c.id = cl.contact_id
    left join unit_categories uc on
      uc.id = u.category_id
    where
      u.property_id in ({property_id})
      and l.status = 1
      and (LAST_DAY(DATE_SUB(@date, INTERVAL 3 MONTH)) >= l.start_date
        and (l.end_date is null
          or l.end_date > LAST_DAY(DATE_SUB(@date, INTERVAL 3 MONTH))))
    group by
      u.id
    UNION ALL
    select
      u.property_id,
      concat(if(isnull(p.number), '', concat(p.number, ' - ')), p.name) as property_name,
      u.status,
      u.id as unit_id,
      l.id as lease_id,
      u.category_id as unit_category_id,
      (
      select
        label
      from
        unit_types
      where
        id = u.unit_type_id) as unit_type,
      (
      select
        name
      from
        unit_types
      where
        id = u.unit_type_id) as unit_type_name,
      upc.id as unit_price_id,
      (cast(
            (
              select (
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
                * 
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
              )
              from units un where un.id = u.id
            )
          as decimal(10, 2)))
          as occupied_sqft,
      ifnull(upc.price, 0) as occupied_base_rent,
      ifnull(upc.set_rate, 0) as occupied_set_rate,
      ifnull(s.price, 0) as occupied_rent,
      TRIM(CONCAT(IFNULL(c.first, ''), ' ', IFNULL(c.last, ''))) as tenant_name,
      u.number,
      u.label,
      uc.description,
      LAST_DAY(DATE_SUB(@date, INTERVAL 4 MONTH)) as date
    from
      units u
    join properties p on
      p.id = u.property_id
    join leases l on
      l.unit_id = u.id
    left join services s on
      s.lease_id = l.id
      and s.product_id = u.product_id
      and s.status = 1
      and (LAST_DAY(DATE_SUB(@date, INTERVAL 4 MONTH)) >= s.start_date
        and (s.end_date is null
          or s.end_date >= LAST_DAY(DATE_SUB(@date, INTERVAL 4 MONTH))))
    left join (
      select
        upc.*
      from
        unit_price_changes upc
      join (
        select
          MAX(upc.id) as max_id
        from
          unit_price_changes upc
        join units u on
          u.id = upc.unit_id
        join properties p on
          p.id = u.property_id
        where
          DATE(CONVERT_TZ(upc.start, '+00:00', p.utc_offset )) <= LAST_DAY(DATE_SUB(@date, INTERVAL 4 MONTH))
            and (upc.end is null
              or DATE(CONVERT_TZ(upc.end, '+00:00', p.utc_offset )) > LAST_DAY(DATE_SUB(@date, INTERVAL 4 MONTH)))
              and u.property_id in ({property_id})
            group by
              upc.unit_id
              ) tupc on
        tupc.max_id = upc.id
            ) upc on
      upc.unit_id = u.id
    left join contact_leases cl on
      cl.lease_id = l.id
      and `primary` = 1
    left join contacts c on
      c.id = cl.contact_id
    left join unit_categories uc on
      uc.id = u.category_id
    where
      u.property_id in ({property_id})
      and l.status = 1
      and (LAST_DAY(DATE_SUB(@date, INTERVAL 4 MONTH)) >= l.start_date
        and (l.end_date is null
          or l.end_date > LAST_DAY(DATE_SUB(@date, INTERVAL 4 MONTH))))
    group by
      u.id
    UNION ALL
    select
      u.property_id,
      concat(if(isnull(p.number), '', concat(p.number, ' - ')), p.name) as property_name,
      u.status,
      u.id as unit_id,
      l.id as lease_id,
      u.category_id as unit_category_id,
      (
      select
        label
      from
        unit_types
      where
        id = u.unit_type_id) as unit_type,
      (
      select
        name
      from
        unit_types
      where
        id = u.unit_type_id) as unit_type_name,
      upc.id as unit_price_id,
      (cast(
            (
              select (
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
                * 
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
              )
              from units un where un.id = u.id
            )
          as decimal(10, 2)))
          as occupied_sqft,
      ifnull(upc.price, 0) as occupied_base_rent,
      ifnull(upc.set_rate, 0) as occupied_set_rate,
      ifnull(s.price, 0) as occupied_rent,
      TRIM(CONCAT(IFNULL(c.first, ''), ' ', IFNULL(c.last, ''))) as tenant_name,
      u.number,
      u.label,
      uc.description,
      LAST_DAY(DATE_SUB(@date, INTERVAL 5 MONTH)) as date
    from
      units u
    join properties p on
      p.id = u.property_id
    join leases l on
      l.unit_id = u.id
    left join services s on
      s.lease_id = l.id
      and s.product_id = u.product_id
      and s.status = 1
      and (LAST_DAY(DATE_SUB(@date, INTERVAL 5 MONTH)) >= s.start_date
        and (s.end_date is null
          or s.end_date >= LAST_DAY(DATE_SUB(@date, INTERVAL 5 MONTH))))
    left join (
      select
        upc.*
      from
        unit_price_changes upc
      join (
        select
          MAX(upc.id) as max_id
        from
          unit_price_changes upc
        join units u on
          u.id = upc.unit_id
        join properties p on
          p.id = u.property_id
        where
          DATE(CONVERT_TZ(upc.start, '+00:00', p.utc_offset )) <= LAST_DAY(DATE_SUB(@date, INTERVAL 5 MONTH))
            and (upc.end is null
              or DATE(CONVERT_TZ(upc.end, '+00:00', p.utc_offset )) > LAST_DAY(DATE_SUB(@date, INTERVAL 5 MONTH)))
              and u.property_id in ({property_id})
            group by
              upc.unit_id
              ) tupc on
        tupc.max_id = upc.id
            ) upc on
      upc.unit_id = u.id
    left join contact_leases cl on
      cl.lease_id = l.id
      and `primary` = 1
    left join contacts c on
      c.id = cl.contact_id
    left join unit_categories uc on
      uc.id = u.category_id
    where
      u.property_id in ({property_id})
      and l.status = 1
      and (LAST_DAY(DATE_SUB(@date, INTERVAL 5 MONTH)) >= l.start_date
        and (l.end_date is null
          or l.end_date > LAST_DAY(DATE_SUB(@date, INTERVAL 5 MONTH))))
    group by
      u.id
    UNION ALL
    select
      u.property_id,
      concat(if(isnull(p.number), '', concat(p.number, ' - ')), p.name) as property_name,
      u.status,
      u.id as unit_id,
      l.id as lease_id,
      u.category_id as unit_category_id,
      (
      select
        label
      from
        unit_types
      where
        id = u.unit_type_id) as unit_type,
      (
      select
        name
      from
        unit_types
      where
        id = u.unit_type_id) as unit_type_name,
      upc.id as unit_price_id,
      (cast(
            (
              select (
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
                * 
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
              )
              from units un where un.id = u.id
            )
          as decimal(10, 2)))
          as occupied_sqft,
      ifnull(upc.price, 0) as occupied_base_rent,
      ifnull(upc.set_rate, 0) as occupied_set_rate,
      ifnull(s.price, 0) as occupied_rent,
      TRIM(CONCAT(IFNULL(c.first, ''), ' ', IFNULL(c.last, ''))) as tenant_name,
      u.number,
      u.label,
      uc.description,
      LAST_DAY(DATE_SUB(@date, INTERVAL 6 MONTH)) as date
    from
      units u
    join properties p on
      p.id = u.property_id
    join leases l on
      l.unit_id = u.id
    left join services s on
      s.lease_id = l.id
      and s.product_id = u.product_id
      and s.status = 1
      and (LAST_DAY(DATE_SUB(@date, INTERVAL 6 MONTH)) >= s.start_date
        and (s.end_date is null
          or s.end_date >= LAST_DAY(DATE_SUB(@date, INTERVAL 6 MONTH))))
    left join (
      select
        upc.*
      from
        unit_price_changes upc
      join (
        select
          MAX(upc.id) as max_id
        from
          unit_price_changes upc
        join units u on
          u.id = upc.unit_id
        join properties p on
          p.id = u.property_id
        where
          DATE(CONVERT_TZ(upc.start, '+00:00', p.utc_offset )) <= LAST_DAY(DATE_SUB(@date, INTERVAL 6 MONTH))
            and (upc.end is null
              or DATE(CONVERT_TZ(upc.end, '+00:00', p.utc_offset )) > LAST_DAY(DATE_SUB(@date, INTERVAL 6 MONTH)))
              and u.property_id in ({property_id})
            group by
              upc.unit_id
              ) tupc on
        tupc.max_id = upc.id
            ) upc on
      upc.unit_id = u.id
    left join contact_leases cl on
      cl.lease_id = l.id
      and `primary` = 1
    left join contacts c on
      c.id = cl.contact_id
    left join unit_categories uc on
      uc.id = u.category_id
    where
      u.property_id in ({property_id})
      and l.status = 1
      and (LAST_DAY(DATE_SUB(@date, INTERVAL 6 MONTH)) >= l.start_date
        and (l.end_date is null
          or l.end_date > LAST_DAY(DATE_SUB(@date, INTERVAL 6 MONTH))))
    group by
      u.id
    UNION ALL
    select
      u.property_id,
      concat(if(isnull(p.number), '', concat(p.number, ' - ')), p.name) as property_name,
      u.status,
      u.id as unit_id,
      l.id as lease_id,
      u.category_id as unit_category_id,
      (
      select
        label
      from
        unit_types
      where
        id = u.unit_type_id) as unit_type,
      (
      select
        name
      from
        unit_types
      where
        id = u.unit_type_id) as unit_type_name,
      upc.id as unit_price_id,
      (cast(
            (
              select (
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
                * 
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
              )
              from units un where un.id = u.id
            )
          as decimal(10, 2)))
          as occupied_sqft,
      ifnull(upc.price, 0) as occupied_base_rent,
      ifnull(upc.set_rate, 0) as occupied_set_rate,
      ifnull(s.price, 0) as occupied_rent,
      TRIM(CONCAT(IFNULL(c.first, ''), ' ', IFNULL(c.last, ''))) as tenant_name,
      u.number,
      u.label,
      uc.description,
      LAST_DAY(DATE_SUB(@date, INTERVAL 7 MONTH)) as date
    from
      units u
    join properties p on
      p.id = u.property_id
    join leases l on
      l.unit_id = u.id
    left join services s on
      s.lease_id = l.id
      and s.product_id = u.product_id
      and s.status = 1
      and (LAST_DAY(DATE_SUB(@date, INTERVAL 7 MONTH)) >= s.start_date
        and (s.end_date is null
          or s.end_date >= LAST_DAY(DATE_SUB(@date, INTERVAL 7 MONTH))))
    left join (
      select
        upc.*
      from
        unit_price_changes upc
      join (
        select
          MAX(upc.id) as max_id
        from
          unit_price_changes upc
        join units u on
          u.id = upc.unit_id
        join properties p on
          p.id = u.property_id
        where
          DATE(CONVERT_TZ(upc.start, '+00:00', p.utc_offset )) <= LAST_DAY(DATE_SUB(@date, INTERVAL 7 MONTH))
            and (upc.end is null
              or DATE(CONVERT_TZ(upc.end, '+00:00', p.utc_offset )) > LAST_DAY(DATE_SUB(@date, INTERVAL 7 MONTH)))
              and u.property_id in ({property_id})
            group by
              upc.unit_id
              ) tupc on
        tupc.max_id = upc.id
            ) upc on
      upc.unit_id = u.id
    left join contact_leases cl on
      cl.lease_id = l.id
      and `primary` = 1
    left join contacts c on
      c.id = cl.contact_id
    left join unit_categories uc on
      uc.id = u.category_id
    where
      u.property_id in ({property_id})
      and l.status = 1
      and (LAST_DAY(DATE_SUB(@date, INTERVAL 7 MONTH)) >= l.start_date
        and (l.end_date is null
          or l.end_date > LAST_DAY(DATE_SUB(@date, INTERVAL 7 MONTH))))
    group by
      u.id
    UNION ALL
    select
      u.property_id,
      concat(if(isnull(p.number), '', concat(p.number, ' - ')), p.name) as property_name,
      u.status,
      u.id as unit_id,
      l.id as lease_id,
      u.category_id as unit_category_id,
      (
      select
        label
      from
        unit_types
      where
        id = u.unit_type_id) as unit_type,
      (
      select
        name
      from
        unit_types
      where
        id = u.unit_type_id) as unit_type_name,
      upc.id as unit_price_id,
      (cast(
            (
              select (
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
                * 
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
              )
              from units un where un.id = u.id
            )
          as decimal(10, 2)))
          as occupied_sqft,
      ifnull(upc.price, 0) as occupied_base_rent,
      ifnull(upc.set_rate, 0) as occupied_set_rate,
      ifnull(s.price, 0) as occupied_rent,
      TRIM(CONCAT(IFNULL(c.first, ''), ' ', IFNULL(c.last, ''))) as tenant_name,
      u.number,
      u.label,
      uc.description,
      LAST_DAY(DATE_SUB(@date, INTERVAL 8 MONTH)) as date
    from
      units u
    join properties p on
      p.id = u.property_id
    join leases l on
      l.unit_id = u.id
    left join services s on
      s.lease_id = l.id
      and s.product_id = u.product_id
      and s.status = 1
      and (LAST_DAY(DATE_SUB(@date, INTERVAL 8 MONTH)) >= s.start_date
        and (s.end_date is null
          or s.end_date >= LAST_DAY(DATE_SUB(@date, INTERVAL 8 MONTH))))
    left join (
      select
        upc.*
      from
        unit_price_changes upc
      join (
        select
          MAX(upc.id) as max_id
        from
          unit_price_changes upc
        join units u on
          u.id = upc.unit_id
        join properties p on
          p.id = u.property_id
        where
          DATE(CONVERT_TZ(upc.start, '+00:00', p.utc_offset )) <= LAST_DAY(DATE_SUB(@date, INTERVAL 8 MONTH))
            and (upc.end is null
              or DATE(CONVERT_TZ(upc.end, '+00:00', p.utc_offset )) > LAST_DAY(DATE_SUB(@date, INTERVAL 8 MONTH)))
              and u.property_id in ({property_id})
            group by
              upc.unit_id
              ) tupc on
        tupc.max_id = upc.id
            ) upc on
      upc.unit_id = u.id
    left join contact_leases cl on
      cl.lease_id = l.id
      and `primary` = 1
    left join contacts c on
      c.id = cl.contact_id
    left join unit_categories uc on
      uc.id = u.category_id
    where
      u.property_id in ({property_id})
      and l.status = 1
      and (LAST_DAY(DATE_SUB(@date, INTERVAL 8 MONTH)) >= l.start_date
        and (l.end_date is null
          or l.end_date > LAST_DAY(DATE_SUB(@date, INTERVAL 8 MONTH))))
    group by
      u.id
    UNION ALL
    select
      u.property_id,
      concat(if(isnull(p.number), '', concat(p.number, ' - ')), p.name) as property_name,
      u.status,
      u.id as unit_id,
      l.id as lease_id,
      u.category_id as unit_category_id,
      (
      select
        label
      from
        unit_types
      where
        id = u.unit_type_id) as unit_type,
      (
      select
        name
      from
        unit_types
      where
        id = u.unit_type_id) as unit_type_name,
      upc.id as unit_price_id,
      (cast(
            (
              select (
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
                * 
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
              )
              from units un where un.id = u.id
            )
          as decimal(10, 2)))
          as occupied_sqft,
      ifnull(upc.price, 0) as occupied_base_rent,
      ifnull(upc.set_rate, 0) as occupied_set_rate,
      ifnull(s.price, 0) as occupied_rent,
      TRIM(CONCAT(IFNULL(c.first, ''), ' ', IFNULL(c.last, ''))) as tenant_name,
      u.number,
      u.label,
      uc.description,
      LAST_DAY(DATE_SUB(@date, INTERVAL 9 MONTH)) as date
    from
      units u
    join properties p on
      p.id = u.property_id
    join leases l on
      l.unit_id = u.id
    left join services s on
      s.lease_id = l.id
      and s.product_id = u.product_id
      and s.status = 1
      and (LAST_DAY(DATE_SUB(@date, INTERVAL 9 MONTH)) >= s.start_date
        and (s.end_date is null
          or s.end_date >= LAST_DAY(DATE_SUB(@date, INTERVAL 9 MONTH))))
    left join (
      select
        upc.*
      from
        unit_price_changes upc
      join (
        select
          MAX(upc.id) as max_id
        from
          unit_price_changes upc
        join units u on
          u.id = upc.unit_id
        join properties p on
          p.id = u.property_id
        where
          DATE(CONVERT_TZ(upc.start, '+00:00', p.utc_offset )) <= LAST_DAY(DATE_SUB(@date, INTERVAL 9 MONTH))
            and (upc.end is null
              or DATE(CONVERT_TZ(upc.end, '+00:00', p.utc_offset )) > LAST_DAY(DATE_SUB(@date, INTERVAL 9 MONTH)))
              and u.property_id in ({property_id})
            group by
              upc.unit_id
              ) tupc on
        tupc.max_id = upc.id
            ) upc on
      upc.unit_id = u.id
    left join contact_leases cl on
      cl.lease_id = l.id
      and `primary` = 1
    left join contacts c on
      c.id = cl.contact_id
    left join unit_categories uc on
      uc.id = u.category_id
    where
      u.property_id in ({property_id})
      and l.status = 1
      and (LAST_DAY(DATE_SUB(@date, INTERVAL 9 MONTH)) >= l.start_date
        and (l.end_date is null
          or l.end_date > LAST_DAY(DATE_SUB(@date, INTERVAL 9 MONTH))))
    group by
      u.id
    UNION ALL
    select
      u.property_id,
      concat(if(isnull(p.number), '', concat(p.number, ' - ')), p.name) as property_name,
      u.status,
      u.id as unit_id,
      l.id as lease_id,
      u.category_id as unit_category_id,
      (
      select
        label
      from
        unit_types
      where
        id = u.unit_type_id) as unit_type,
      (
      select
        name
      from
        unit_types
      where
        id = u.unit_type_id) as unit_type_name,
      upc.id as unit_price_id,
      (cast(
            (
              select (
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
                * 
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
              )
              from units un where un.id = u.id
            )
          as decimal(10, 2)))
          as occupied_sqft,
      ifnull(upc.price, 0) as occupied_base_rent,
      ifnull(upc.set_rate, 0) as occupied_set_rate,
      ifnull(s.price, 0) as occupied_rent,
      TRIM(CONCAT(IFNULL(c.first, ''), ' ', IFNULL(c.last, ''))) as tenant_name,
      u.number,
      u.label,
      uc.description,
      LAST_DAY(DATE_SUB(@date, INTERVAL 10 MONTH)) as date
    from
      units u
    join properties p on
      p.id = u.property_id
    join leases l on
      l.unit_id = u.id
    left join services s on
      s.lease_id = l.id
      and s.product_id = u.product_id
      and s.status = 1
      and (LAST_DAY(DATE_SUB(@date, INTERVAL 10 MONTH)) >= s.start_date
        and (s.end_date is null
          or s.end_date >= LAST_DAY(DATE_SUB(@date, INTERVAL 10 MONTH))))
    left join (
      select
        upc.*
      from
        unit_price_changes upc
      join (
        select
          MAX(upc.id) as max_id
        from
          unit_price_changes upc
        join units u on
          u.id = upc.unit_id
        join properties p on
          p.id = u.property_id
        where
          DATE(CONVERT_TZ(upc.start, '+00:00', p.utc_offset )) <= LAST_DAY(DATE_SUB(@date, INTERVAL 10 MONTH))
            and (upc.end is null
              or DATE(CONVERT_TZ(upc.end, '+00:00', p.utc_offset )) > LAST_DAY(DATE_SUB(@date, INTERVAL 10 MONTH)))
              and u.property_id in ({property_id})
            group by
              upc.unit_id
              ) tupc on
        tupc.max_id = upc.id
            ) upc on
      upc.unit_id = u.id
    left join contact_leases cl on
      cl.lease_id = l.id
      and `primary` = 1
    left join contacts c on
      c.id = cl.contact_id
    left join unit_categories uc on
      uc.id = u.category_id
    where
      u.property_id in ({property_id})
      and l.status = 1
      and (LAST_DAY(DATE_SUB(@date, INTERVAL 10 MONTH)) >= l.start_date
        and (l.end_date is null
          or l.end_date > LAST_DAY(DATE_SUB(@date, INTERVAL 10 MONTH))))
    group by
      u.id
    UNION ALL
    select
      u.property_id,
      concat(if(isnull(p.number), '', concat(p.number, ' - ')), p.name) as property_name,
      u.status,
      u.id as unit_id,
      l.id as lease_id,
      u.category_id as unit_category_id,
      (
      select
        label
      from
        unit_types
      where
        id = u.unit_type_id) as unit_type,
      (
      select
        name
      from
        unit_types
      where
        id = u.unit_type_id) as unit_type_name,
      upc.id as unit_price_id,
      (cast(
            (
              select (
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
                * 
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
              )
              from units un where un.id = u.id
            )
          as decimal(10, 2)))
        as occupied_sqft,
      ifnull(upc.price, 0) as occupied_base_rent,
      ifnull(upc.set_rate, 0) as occupied_set_rate,
      ifnull(s.price, 0) as occupied_rent,
      TRIM(CONCAT(IFNULL(c.first, ''), ' ', IFNULL(c.last, ''))) as tenant_name,
      u.number,
      u.label,
      uc.description,
      LAST_DAY(DATE_SUB(@date, INTERVAL 11 MONTH)) as date
    from
      units u
    join properties p on
      p.id = u.property_id
    join leases l on
      l.unit_id = u.id
    left join services s on
      s.lease_id = l.id
      and s.product_id = u.product_id
      and s.status = 1
      and (LAST_DAY(DATE_SUB(@date, INTERVAL 11 MONTH)) >= s.start_date
        and (s.end_date is null
          or s.end_date >= LAST_DAY(DATE_SUB(@date, INTERVAL 11 MONTH))))
    left join (
      select
        upc.*
      from
        unit_price_changes upc
      join (
        select
          MAX(upc.id) as max_id
        from
          unit_price_changes upc
        join units u on
          u.id = upc.unit_id
        join properties p on
          p.id = u.property_id
        where
          DATE(CONVERT_TZ(upc.start, '+00:00', p.utc_offset )) <= LAST_DAY(DATE_SUB(@date, INTERVAL 11 MONTH))
            and (upc.end is null
              or DATE(CONVERT_TZ(upc.end, '+00:00', p.utc_offset )) > LAST_DAY(DATE_SUB(@date, INTERVAL 11 MONTH)))
              and u.property_id in ({property_id})
            group by
              upc.unit_id
              ) tupc on
        tupc.max_id = upc.id
            ) upc on
      upc.unit_id = u.id
    left join contact_leases cl on
      cl.lease_id = l.id
      and `primary` = 1
    left join contacts c on
      c.id = cl.contact_id
    left join unit_categories uc on
      uc.id = u.category_id
    where
      u.property_id in ({property_id})
      and l.status = 1
      and (LAST_DAY(DATE_SUB(@date, INTERVAL 11 MONTH)) >= l.start_date
        and (l.end_date is null
          or l.end_date > LAST_DAY(DATE_SUB(@date, INTERVAL 11 MONTH))))
    group by
      u.id
    UNION ALL
    select
      u.property_id,
      concat(if(isnull(p.number), '', concat(p.number, ' - ')), p.name) as property_name,
      u.status,
      u.id as unit_id,
      l.id as lease_id,
      u.category_id as unit_category_id,
      (
      select
        label
      from
        unit_types
      where
        id = u.unit_type_id) as unit_type,
      (
      select
        name
      from
        unit_types
      where
        id = u.unit_type_id) as unit_type_name,
      upc.id as unit_price_id,
      (cast(
            (
              select (
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
                * 
                ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
              )
              from units un where un.id = u.id
            )
          as decimal(10, 2)))
          as occupied_sqft,
      ifnull(upc.price, 0) as occupied_base_rent,
      ifnull(upc.set_rate, 0) as occupied_set_rate,
      ifnull(s.price, 0) as occupied_rent,
      TRIM(CONCAT(IFNULL(c.first, ''), ' ', IFNULL(c.last, ''))) as tenant_name,
      u.number,
      u.label,
      uc.description,
      LAST_DAY(DATE_SUB(@date, INTERVAL 12 MONTH)) as date
    from
      units u
    join properties p on
      p.id = u.property_id
    join leases l on
      l.unit_id = u.id
    left join services s on
      s.lease_id = l.id
      and s.product_id = u.product_id
      and s.status = 1
      and (LAST_DAY(DATE_SUB(@date, INTERVAL 12 MONTH)) >= s.start_date
        and (s.end_date is null
          or s.end_date >= LAST_DAY(DATE_SUB(@date, INTERVAL 12 MONTH))))
    left join (
      select
        upc.*
      from
        unit_price_changes upc
      join (
        select
          MAX(upc.id) as max_id
        from
          unit_price_changes upc
        join units u on
          u.id = upc.unit_id
        join properties p on
          p.id = u.property_id
        where
          DATE(CONVERT_TZ(upc.start, '+00:00', p.utc_offset )) <= LAST_DAY(DATE_SUB(@date, INTERVAL 12 MONTH))
            and (upc.end is null
              or DATE(CONVERT_TZ(upc.end, '+00:00', p.utc_offset )) > LAST_DAY(DATE_SUB(@date, INTERVAL 12 MONTH)))
              and u.property_id in ({property_id})
            group by
              upc.unit_id
              ) tupc on
        tupc.max_id = upc.id
            ) upc on
      upc.unit_id = u.id
    left join contact_leases cl on
      cl.lease_id = l.id
      and `primary` = 1
    left join contacts c on
      c.id = cl.contact_id
    left join unit_categories uc on
      uc.id = u.category_id
    where
      u.property_id in ({property_id})
      and l.status = 1
      and (LAST_DAY(DATE_SUB(@date, INTERVAL 12 MONTH)) >= l.start_date
        and (l.end_date is null
          or l.end_date > LAST_DAY(DATE_SUB(@date, INTERVAL 12 MONTH))))
    group by
      u.id
        )
        SELECT
        property_id,
      DATE_FORMAT(ou.date, '%M-%y') as occupied_month,
      count(ou.unit_id) as occupied_count,
      sum(cast(ou.occupied_sqft as decimal(10, 2))) as occupied_space_area
      FROM
      occupied_units ou
    GROUP BY property_id, DATE_FORMAT(ou.date, '%M-%y')
    ORDER BY property_id,occupied_month;
     """
 
  redshift_df = fetch_data(redshift_engine, redshift_query)
  mysql_df = execute_mysql_statements(mysql_engine, mysql_query)

  # mysql_df['date'] = mysql_df['month'].apply(convert_to_last_day)

  redshift_grouped = redshift_df
  mysql_grouped = mysql_df

  redshift_grouped['occupied_month'] = redshift_grouped['occupied_month'].str.replace(' ', '', regex=False)  

  # redshift_grouped.rename(columns={'date_dt':'date'},inplace=True)
  # redshift_grouped['date'] = pd.to_datetime(redshift_grouped['date'])
  # mysql_grouped['date'] = pd.to_datetime(mysql_grouped['date'])

  # redshift_grouped['month_year'] = redshift_grouped['date'].dt.strftime('%b %y')
  # mysql_grouped['month_year'] = mysql_grouped['date'].dt.strftime('%b %y')
  # redshift_grouped = redshift_grouped.drop('date',axis = 1)
  # mysql_grouped = mysql_grouped.drop('date',axis = 1)


  redshift_grouped = redshift_grouped.add_suffix('_tdw')
  mysql_grouped = mysql_grouped.add_suffix('_rds')

  redshift_grouped.rename(columns={'property_id_tdw':'property_id','occupied_month_tdw':'occupied_month'},inplace=True)
  mysql_grouped.rename(columns={'property_id_rds':'property_id','occupied_month_rds':'occupied_month'},inplace=True)
  if redshift_df.empty and mysql_df.empty:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','occupied_month'], how='outer', indicator=True)
    return comparison_df
  else:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on=['property_id','occupied_month'], how='outer', indicator=True)
    comparison_df['_merge'] = comparison_df['_merge'].map({'left_only': 'TDW', 'right_only': 'RDS', 'both': 'Both'})
    comparison_df['om'] = pd.to_datetime(comparison_df['occupied_month'],format='%B-%y')
    comparison_df = comparison_df.sort_values(['property_id','om'],ascending=[True,False])
    comparison_df = comparison_df.drop(columns=['om'])

    comparison_df.fillna({'occupied_count_tdw': 0, 'occupied_count_rds': 0,'occupied_space_area_tdw':0,'occupied_space_area_rds':0}, inplace=True)
    # Determine the status
    comparison_df['Status'] = comparison_df.apply(
        lambda row: 'match' if row['occupied_count_tdw'] == row['occupied_count_rds'] 
        and row['occupied_space_area_tdw'] == row['occupied_space_area_rds']
        else 'mismatch', axis=1
    )
    
    return comparison_df
  

def Complimentary_Units(redshift_engine,mysql_engine,tdw_date,rds_date):
  print("Complimentary query running")
  
  date_conditions=setDate_For_Occupancy(tdw_date)

  redshift_query = f"""
  SELECT tpdv.src_property_id as property_id, 
  to_char(dd.date_dt,'Month-YY') as complimentary_month, 
  COUNT(dd.date_dt) as complimentary_count,
  sum(spv.space_area) as complimentary_space_area
  FROM tdw_ro.tdw_occupancy_by_space_pf_v spv 
  join tdw_ro.tdw_property_d_v tpdv on spv.property_sk = tpdv.property_sk
  JOIN tdw_ro.tdw_date_d_v dd ON spv.date_sk = dd.date_sk
  WHERE
  spv.property_sk in ({property_sk}) AND
  ({date_conditions}) AND 
  spv.complementary_space_count  = 1
  GROUP BY tpdv.src_property_id,to_char(dd.date_dt,'Month-YY')
  ORDER BY tpdv.src_property_id,to_char(dd.date_dt,'Month-YY');
  """

  mysql_query = f"""
use hummingbird;
set @date = '{rds_date}';

-- Occupied and Complimentary	

  WITH occupied_units as (
  select
    u.property_id,
    concat(if(isnull(p.number), '', concat(p.number, ' - ')), p.name) as property_name,
    u.status,
    u.id as unit_id,
    l.id as lease_id,
    u.category_id as unit_category_id,
    (
    select
      label
    from
      unit_types
    where
      id = u.unit_type_id) as unit_type,
    (
    select
      name
    from
      unit_types
    where
      id = u.unit_type_id) as unit_type_name,
    upc.id as unit_price_id,
    (cast(
          (
            select (
              ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
              * 
              ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
            )
            from units un where un.id = u.id
          )
        as decimal(10, 2)))
        as occupied_sqft,
    ifnull(upc.price, 0) as occupied_base_rent,
    ifnull(upc.set_rate, 0) as occupied_set_rate,
    ifnull(s.price, 0) as occupied_rent,
    TRIM(CONCAT(IFNULL(c.first, ''), ' ', IFNULL(c.last, ''))) as tenant_name,
    u.number,
    u.label,
    uc.description,
    @date as date
  from
    units u
  join properties p on
    p.id = u.property_id
  join leases l on
    l.unit_id = u.id
  left join services s on
    s.lease_id = l.id
    and s.product_id = u.product_id
    and s.status = 1
    and (LAST_DAY(@date) >= s.start_date
      and (s.end_date is null
        or s.end_date >= LAST_DAY(@date)))
  left join (
    select
      upc.*
    from
      unit_price_changes upc
    join (
      select
        MAX(upc.id) as max_id
      from
        unit_price_changes upc
      join units u on
        u.id = upc.unit_id
      join properties p on
        p.id = u.property_id
      where
        DATE(CONVERT_TZ(upc.start, '+00:00', p.utc_offset )) <= LAST_DAY(@date)
          and (upc.end is null
            or DATE(CONVERT_TZ(upc.end, '+00:00', p.utc_offset )) > LAST_DAY(@date))
            and u.property_id in ({property_id})
          group by
            upc.unit_id
            ) tupc on
      tupc.max_id = upc.id
          ) upc on
    upc.unit_id = u.id
  left join contact_leases cl on
    cl.lease_id = l.id
    and `primary` = 1
  left join contacts c on
    c.id = cl.contact_id
  left join unit_categories uc on
    uc.id = u.category_id
  where
    u.property_id in ({property_id})
    and l.status = 1
    and (LAST_DAY(@date) >= l.start_date
      and (l.end_date is null
        or l.end_date > LAST_DAY(@date)))
  group by
    u.id
  UNION ALL
  select
    u.property_id,
    concat(if(isnull(p.number), '', concat(p.number, ' - ')), p.name) as property_name,
    u.status,
    u.id as unit_id,
    l.id as lease_id,
    u.category_id as unit_category_id,
    (
    select
      label
    from
      unit_types
    where
      id = u.unit_type_id) as unit_type,
    (
    select
      name
    from
      unit_types
    where
      id = u.unit_type_id) as unit_type_name,
    upc.id as unit_price_id,
    (cast(
          (
            select (
              ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
              * 
              ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
            )
            from units un where un.id = u.id
          )
        as decimal(10, 2)))
        as occupied_sqft,
    ifnull(upc.price, 0) as occupied_base_rent,
    ifnull(upc.set_rate, 0) as occupied_set_rate,
    ifnull(s.price, 0) as occupied_rent,
    TRIM(CONCAT(IFNULL(c.first, ''), ' ', IFNULL(c.last, ''))) as tenant_name,
    u.number,
    u.label,
    uc.description,
    LAST_DAY(DATE_SUB(@date, INTERVAL 1 MONTH)) as date
  from
    units u
  join properties p on
    p.id = u.property_id
  join leases l on
    l.unit_id = u.id
  left join services s on
    s.lease_id = l.id
    and s.product_id = u.product_id
    and s.status = 1
    and (LAST_DAY(DATE_SUB(@date, INTERVAL 1 MONTH)) >= s.start_date
      and (s.end_date is null
        or s.end_date >= LAST_DAY(DATE_SUB(@date, INTERVAL 1 MONTH))))
  left join (
    select
      upc.*
    from
      unit_price_changes upc
    join (
      select
        MAX(upc.id) as max_id
      from
        unit_price_changes upc
      join units u on
        u.id = upc.unit_id
      join properties p on
        p.id = u.property_id
      where
        DATE(CONVERT_TZ(upc.start, '+00:00', p.utc_offset )) <= LAST_DAY(DATE_SUB(@date, INTERVAL 1 MONTH))
          and (upc.end is null
            or DATE(CONVERT_TZ(upc.end, '+00:00', p.utc_offset )) > LAST_DAY(DATE_SUB(@date, INTERVAL 1 MONTH)))
            and u.property_id in ({property_id})
          group by
            upc.unit_id
            ) tupc on
      tupc.max_id = upc.id
          ) upc on
    upc.unit_id = u.id
  left join contact_leases cl on
    cl.lease_id = l.id
    and `primary` = 1
  left join contacts c on
    c.id = cl.contact_id
  left join unit_categories uc on
    uc.id = u.category_id
  where
    u.property_id in ({property_id})
    and l.status = 1
    and (LAST_DAY(DATE_SUB(@date, INTERVAL 1 MONTH)) >= l.start_date
      and (l.end_date is null
        or l.end_date > LAST_DAY(DATE_SUB(@date, INTERVAL 1 MONTH))))
  group by
    u.id
  UNION ALL
  select
    u.property_id,
    concat(if(isnull(p.number), '', concat(p.number, ' - ')), p.name) as property_name,
    u.status,
    u.id as unit_id,
    l.id as lease_id,
    u.category_id as unit_category_id,
    (
    select
      label
    from
      unit_types
    where
      id = u.unit_type_id) as unit_type,
    (
    select
      name
    from
      unit_types
    where
      id = u.unit_type_id) as unit_type_name,
    upc.id as unit_price_id,
    (cast(
          (
            select (
              ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
              * 
              ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
            )
            from units un where un.id = u.id
          )
        as decimal(10, 2)))
        as occupied_sqft,
    ifnull(upc.price, 0) as occupied_base_rent,
    ifnull(upc.set_rate, 0) as occupied_set_rate,
    ifnull(s.price, 0) as occupied_rent,
    TRIM(CONCAT(IFNULL(c.first, ''), ' ', IFNULL(c.last, ''))) as tenant_name,
    u.number,
    u.label,
    uc.description,
    LAST_DAY(DATE_SUB(@date, INTERVAL 2 MONTH)) as date
  from
    units u
  join properties p on
    p.id = u.property_id
  join leases l on
    l.unit_id = u.id
  left join services s on
    s.lease_id = l.id
    and s.product_id = u.product_id
    and s.status = 1
    and (LAST_DAY(DATE_SUB(@date, INTERVAL 2 MONTH)) >= s.start_date
      and (s.end_date is null
        or s.end_date >= LAST_DAY(DATE_SUB(@date, INTERVAL 2 MONTH))))
  left join (
    select
      upc.*
    from
      unit_price_changes upc
    join (
      select
        MAX(upc.id) as max_id
      from
        unit_price_changes upc
      join units u on
        u.id = upc.unit_id
      join properties p on
        p.id = u.property_id
      where
        DATE(CONVERT_TZ(upc.start, '+00:00', p.utc_offset )) <= LAST_DAY(DATE_SUB(@date, INTERVAL 2 MONTH))
          and (upc.end is null
            or DATE(CONVERT_TZ(upc.end, '+00:00', p.utc_offset )) > LAST_DAY(DATE_SUB(@date, INTERVAL 2 MONTH)))
            and u.property_id in ({property_id})
          group by
            upc.unit_id
            ) tupc on
      tupc.max_id = upc.id
          ) upc on
    upc.unit_id = u.id
  left join contact_leases cl on
    cl.lease_id = l.id
    and `primary` = 1
  left join contacts c on
    c.id = cl.contact_id
  left join unit_categories uc on
    uc.id = u.category_id
  where
    u.property_id in ({property_id})
    and l.status = 1
    and (LAST_DAY(DATE_SUB(@date, INTERVAL 2 MONTH)) >= l.start_date
      and (l.end_date is null
        or l.end_date > LAST_DAY(DATE_SUB(@date, INTERVAL 2 MONTH))))
  group by
    u.id
  UNION ALL
  select
    u.property_id,
    concat(if(isnull(p.number), '', concat(p.number, ' - ')), p.name) as property_name,
    u.status,
    u.id as unit_id,
    l.id as lease_id,
    u.category_id as unit_category_id,
    (
    select
      label
    from
      unit_types
    where
      id = u.unit_type_id) as unit_type,
    (
    select
      name
    from
      unit_types
    where
      id = u.unit_type_id) as unit_type_name,
    upc.id as unit_price_id,
    (cast(
          (
            select (
              ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
              * 
              ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
            )
            from units un where un.id = u.id
          )
        as decimal(10, 2)))
        as occupied_sqft,
    ifnull(upc.price, 0) as occupied_base_rent,
    ifnull(upc.set_rate, 0) as occupied_set_rate,
    ifnull(s.price, 0) as occupied_rent,
    TRIM(CONCAT(IFNULL(c.first, ''), ' ', IFNULL(c.last, ''))) as tenant_name,
    u.number,
    u.label,
    uc.description,
    LAST_DAY(DATE_SUB(@date, INTERVAL 3 MONTH)) as date
  from
    units u
  join properties p on
    p.id = u.property_id
  join leases l on
    l.unit_id = u.id
  left join services s on
    s.lease_id = l.id
    and s.product_id = u.product_id
    and s.status = 1
    and (LAST_DAY(DATE_SUB(@date, INTERVAL 3 MONTH)) >= s.start_date
      and (s.end_date is null
        or s.end_date >= LAST_DAY(DATE_SUB(@date, INTERVAL 3 MONTH))))
  left join (
    select
      upc.*
    from
      unit_price_changes upc
    join (
      select
        MAX(upc.id) as max_id
      from
        unit_price_changes upc
      join units u on
        u.id = upc.unit_id
      join properties p on
        p.id = u.property_id
      where
        DATE(CONVERT_TZ(upc.start, '+00:00', p.utc_offset )) <= LAST_DAY(DATE_SUB(@date, INTERVAL 3 MONTH))
          and (upc.end is null
            or DATE(CONVERT_TZ(upc.end, '+00:00', p.utc_offset )) > LAST_DAY(DATE_SUB(@date, INTERVAL 3 MONTH)))
            and u.property_id in ({property_id})
          group by
            upc.unit_id
            ) tupc on
      tupc.max_id = upc.id
          ) upc on
    upc.unit_id = u.id
  left join contact_leases cl on
    cl.lease_id = l.id
    and `primary` = 1
  left join contacts c on
    c.id = cl.contact_id
  left join unit_categories uc on
    uc.id = u.category_id
  where
    u.property_id in ({property_id})
    and l.status = 1
    and (LAST_DAY(DATE_SUB(@date, INTERVAL 3 MONTH)) >= l.start_date
      and (l.end_date is null
        or l.end_date > LAST_DAY(DATE_SUB(@date, INTERVAL 3 MONTH))))
  group by
    u.id
  UNION ALL
  select
    u.property_id,
    concat(if(isnull(p.number), '', concat(p.number, ' - ')), p.name) as property_name,
    u.status,
    u.id as unit_id,
    l.id as lease_id,
    u.category_id as unit_category_id,
    (
    select
      label
    from
      unit_types
    where
      id = u.unit_type_id) as unit_type,
    (
    select
      name
    from
      unit_types
    where
      id = u.unit_type_id) as unit_type_name,
    upc.id as unit_price_id,
    (cast(
          (
            select (
              ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
              * 
              ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
            )
            from units un where un.id = u.id
          )
        as decimal(10, 2)))
        as occupied_sqft,
    ifnull(upc.price, 0) as occupied_base_rent,
    ifnull(upc.set_rate, 0) as occupied_set_rate,
    ifnull(s.price, 0) as occupied_rent,
    TRIM(CONCAT(IFNULL(c.first, ''), ' ', IFNULL(c.last, ''))) as tenant_name,
    u.number,
    u.label,
    uc.description,
    LAST_DAY(DATE_SUB(@date, INTERVAL 4 MONTH)) as date
  from
    units u
  join properties p on
    p.id = u.property_id
  join leases l on
    l.unit_id = u.id
  left join services s on
    s.lease_id = l.id
    and s.product_id = u.product_id
    and s.status = 1
    and (LAST_DAY(DATE_SUB(@date, INTERVAL 4 MONTH)) >= s.start_date
      and (s.end_date is null
        or s.end_date >= LAST_DAY(DATE_SUB(@date, INTERVAL 4 MONTH))))
  left join (
    select
      upc.*
    from
      unit_price_changes upc
    join (
      select
        MAX(upc.id) as max_id
      from
        unit_price_changes upc
      join units u on
        u.id = upc.unit_id
      join properties p on
        p.id = u.property_id
      where
        DATE(CONVERT_TZ(upc.start, '+00:00', p.utc_offset )) <= LAST_DAY(DATE_SUB(@date, INTERVAL 4 MONTH))
          and (upc.end is null
            or DATE(CONVERT_TZ(upc.end, '+00:00', p.utc_offset )) > LAST_DAY(DATE_SUB(@date, INTERVAL 4 MONTH)))
            and u.property_id in ({property_id})
          group by
            upc.unit_id
            ) tupc on
      tupc.max_id = upc.id
          ) upc on
    upc.unit_id = u.id
  left join contact_leases cl on
    cl.lease_id = l.id
    and `primary` = 1
  left join contacts c on
    c.id = cl.contact_id
  left join unit_categories uc on
    uc.id = u.category_id
  where
    u.property_id in ({property_id})
    and l.status = 1
    and (LAST_DAY(DATE_SUB(@date, INTERVAL 4 MONTH)) >= l.start_date
      and (l.end_date is null
        or l.end_date > LAST_DAY(DATE_SUB(@date, INTERVAL 4 MONTH))))
  group by
    u.id
  UNION ALL
  select
    u.property_id,
    concat(if(isnull(p.number), '', concat(p.number, ' - ')), p.name) as property_name,
    u.status,
    u.id as unit_id,
    l.id as lease_id,
    u.category_id as unit_category_id,
    (
    select
      label
    from
      unit_types
    where
      id = u.unit_type_id) as unit_type,
    (
    select
      name
    from
      unit_types
    where
      id = u.unit_type_id) as unit_type_name,
    upc.id as unit_price_id,
    (cast(
          (
            select (
              ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
              * 
              ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
            )
            from units un where un.id = u.id
          )
        as decimal(10, 2)))
        as occupied_sqft,
    ifnull(upc.price, 0) as occupied_base_rent,
    ifnull(upc.set_rate, 0) as occupied_set_rate,
    ifnull(s.price, 0) as occupied_rent,
    TRIM(CONCAT(IFNULL(c.first, ''), ' ', IFNULL(c.last, ''))) as tenant_name,
    u.number,
    u.label,
    uc.description,
    LAST_DAY(DATE_SUB(@date, INTERVAL 5 MONTH)) as date
  from
    units u
  join properties p on
    p.id = u.property_id
  join leases l on
    l.unit_id = u.id
  left join services s on
    s.lease_id = l.id
    and s.product_id = u.product_id
    and s.status = 1
    and (LAST_DAY(DATE_SUB(@date, INTERVAL 5 MONTH)) >= s.start_date
      and (s.end_date is null
        or s.end_date >= LAST_DAY(DATE_SUB(@date, INTERVAL 5 MONTH))))
  left join (
    select
      upc.*
    from
      unit_price_changes upc
    join (
      select
        MAX(upc.id) as max_id
      from
        unit_price_changes upc
      join units u on
        u.id = upc.unit_id
      join properties p on
        p.id = u.property_id
      where
        DATE(CONVERT_TZ(upc.start, '+00:00', p.utc_offset )) <= LAST_DAY(DATE_SUB(@date, INTERVAL 5 MONTH))
          and (upc.end is null
            or DATE(CONVERT_TZ(upc.end, '+00:00', p.utc_offset )) > LAST_DAY(DATE_SUB(@date, INTERVAL 5 MONTH)))
            and u.property_id in ({property_id})
          group by
            upc.unit_id
            ) tupc on
      tupc.max_id = upc.id
          ) upc on
    upc.unit_id = u.id
  left join contact_leases cl on
    cl.lease_id = l.id
    and `primary` = 1
  left join contacts c on
    c.id = cl.contact_id
  left join unit_categories uc on
    uc.id = u.category_id
  where
    u.property_id in ({property_id})
    and l.status = 1
    and (LAST_DAY(DATE_SUB(@date, INTERVAL 5 MONTH)) >= l.start_date
      and (l.end_date is null
        or l.end_date > LAST_DAY(DATE_SUB(@date, INTERVAL 5 MONTH))))
  group by
    u.id
  UNION ALL
  select
    u.property_id,
    concat(if(isnull(p.number), '', concat(p.number, ' - ')), p.name) as property_name,
    u.status,
    u.id as unit_id,
    l.id as lease_id,
    u.category_id as unit_category_id,
    (
    select
      label
    from
      unit_types
    where
      id = u.unit_type_id) as unit_type,
    (
    select
      name
    from
      unit_types
    where
      id = u.unit_type_id) as unit_type_name,
    upc.id as unit_price_id,
    (cast(
          (
            select (
              ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
              * 
              ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
            )
            from units un where un.id = u.id
          )
        as decimal(10, 2)))
        as occupied_sqft,
    ifnull(upc.price, 0) as occupied_base_rent,
    ifnull(upc.set_rate, 0) as occupied_set_rate,
    ifnull(s.price, 0) as occupied_rent,
    TRIM(CONCAT(IFNULL(c.first, ''), ' ', IFNULL(c.last, ''))) as tenant_name,
    u.number,
    u.label,
    uc.description,
    LAST_DAY(DATE_SUB(@date, INTERVAL 6 MONTH)) as date
  from
    units u
  join properties p on
    p.id = u.property_id
  join leases l on
    l.unit_id = u.id
  left join services s on
    s.lease_id = l.id
    and s.product_id = u.product_id
    and s.status = 1
    and (LAST_DAY(DATE_SUB(@date, INTERVAL 6 MONTH)) >= s.start_date
      and (s.end_date is null
        or s.end_date >= LAST_DAY(DATE_SUB(@date, INTERVAL 6 MONTH))))
  left join (
    select
      upc.*
    from
      unit_price_changes upc
    join (
      select
        MAX(upc.id) as max_id
      from
        unit_price_changes upc
      join units u on
        u.id = upc.unit_id
      join properties p on
        p.id = u.property_id
      where
        DATE(CONVERT_TZ(upc.start, '+00:00', p.utc_offset )) <= LAST_DAY(DATE_SUB(@date, INTERVAL 6 MONTH))
          and (upc.end is null
            or DATE(CONVERT_TZ(upc.end, '+00:00', p.utc_offset )) > LAST_DAY(DATE_SUB(@date, INTERVAL 6 MONTH)))
            and u.property_id in ({property_id})
          group by
            upc.unit_id
            ) tupc on
      tupc.max_id = upc.id
          ) upc on
    upc.unit_id = u.id
  left join contact_leases cl on
    cl.lease_id = l.id
    and `primary` = 1
  left join contacts c on
    c.id = cl.contact_id
  left join unit_categories uc on
    uc.id = u.category_id
  where
    u.property_id in ({property_id})
    and l.status = 1
    and (LAST_DAY(DATE_SUB(@date, INTERVAL 6 MONTH)) >= l.start_date
      and (l.end_date is null
        or l.end_date > LAST_DAY(DATE_SUB(@date, INTERVAL 6 MONTH))))
  group by
    u.id
  UNION ALL
  select
    u.property_id,
    concat(if(isnull(p.number), '', concat(p.number, ' - ')), p.name) as property_name,
    u.status,
    u.id as unit_id,
    l.id as lease_id,
    u.category_id as unit_category_id,
    (
    select
      label
    from
      unit_types
    where
      id = u.unit_type_id) as unit_type,
    (
    select
      name
    from
      unit_types
    where
      id = u.unit_type_id) as unit_type_name,
    upc.id as unit_price_id,
    (cast(
          (
            select (
              ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
              * 
              ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
            )
            from units un where un.id = u.id
          )
        as decimal(10, 2)))
        as occupied_sqft,
    ifnull(upc.price, 0) as occupied_base_rent,
    ifnull(upc.set_rate, 0) as occupied_set_rate,
    ifnull(s.price, 0) as occupied_rent,
    TRIM(CONCAT(IFNULL(c.first, ''), ' ', IFNULL(c.last, ''))) as tenant_name,
    u.number,
    u.label,
    uc.description,
    LAST_DAY(DATE_SUB(@date, INTERVAL 7 MONTH)) as date
  from
    units u
  join properties p on
    p.id = u.property_id
  join leases l on
    l.unit_id = u.id
  left join services s on
    s.lease_id = l.id
    and s.product_id = u.product_id
    and s.status = 1
    and (LAST_DAY(DATE_SUB(@date, INTERVAL 7 MONTH)) >= s.start_date
      and (s.end_date is null
        or s.end_date >= LAST_DAY(DATE_SUB(@date, INTERVAL 7 MONTH))))
  left join (
    select
      upc.*
    from
      unit_price_changes upc
    join (
      select
        MAX(upc.id) as max_id
      from
        unit_price_changes upc
      join units u on
        u.id = upc.unit_id
      join properties p on
        p.id = u.property_id
      where
        DATE(CONVERT_TZ(upc.start, '+00:00', p.utc_offset )) <= LAST_DAY(DATE_SUB(@date, INTERVAL 7 MONTH))
          and (upc.end is null
            or DATE(CONVERT_TZ(upc.end, '+00:00', p.utc_offset )) > LAST_DAY(DATE_SUB(@date, INTERVAL 7 MONTH)))
            and u.property_id in ({property_id})
          group by
            upc.unit_id
            ) tupc on
      tupc.max_id = upc.id
          ) upc on
    upc.unit_id = u.id
  left join contact_leases cl on
    cl.lease_id = l.id
    and `primary` = 1
  left join contacts c on
    c.id = cl.contact_id
  left join unit_categories uc on
    uc.id = u.category_id
  where
    u.property_id in ({property_id})
    and l.status = 1
    and (LAST_DAY(DATE_SUB(@date, INTERVAL 7 MONTH)) >= l.start_date
      and (l.end_date is null
        or l.end_date > LAST_DAY(DATE_SUB(@date, INTERVAL 7 MONTH))))
  group by
    u.id
  UNION ALL
  select
    u.property_id,
    concat(if(isnull(p.number), '', concat(p.number, ' - ')), p.name) as property_name,
    u.status,
    u.id as unit_id,
    l.id as lease_id,
    u.category_id as unit_category_id,
    (
    select
      label
    from
      unit_types
    where
      id = u.unit_type_id) as unit_type,
    (
    select
      name
    from
      unit_types
    where
      id = u.unit_type_id) as unit_type_name,
    upc.id as unit_price_id,
    (cast(
          (
            select (
              ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
              * 
              ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
            )
            from units un where un.id = u.id
          )
        as decimal(10, 2)))
        as occupied_sqft,
    ifnull(upc.price, 0) as occupied_base_rent,
    ifnull(upc.set_rate, 0) as occupied_set_rate,
    ifnull(s.price, 0) as occupied_rent,
    TRIM(CONCAT(IFNULL(c.first, ''), ' ', IFNULL(c.last, ''))) as tenant_name,
    u.number,
    u.label,
    uc.description,
    LAST_DAY(DATE_SUB(@date, INTERVAL 8 MONTH)) as date
  from
    units u
  join properties p on
    p.id = u.property_id
  join leases l on
    l.unit_id = u.id
  left join services s on
    s.lease_id = l.id
    and s.product_id = u.product_id
    and s.status = 1
    and (LAST_DAY(DATE_SUB(@date, INTERVAL 8 MONTH)) >= s.start_date
      and (s.end_date is null
        or s.end_date >= LAST_DAY(DATE_SUB(@date, INTERVAL 8 MONTH))))
  left join (
    select
      upc.*
    from
      unit_price_changes upc
    join (
      select
        MAX(upc.id) as max_id
      from
        unit_price_changes upc
      join units u on
        u.id = upc.unit_id
      join properties p on
        p.id = u.property_id
      where
        DATE(CONVERT_TZ(upc.start, '+00:00', p.utc_offset )) <= LAST_DAY(DATE_SUB(@date, INTERVAL 8 MONTH))
          and (upc.end is null
            or DATE(CONVERT_TZ(upc.end, '+00:00', p.utc_offset )) > LAST_DAY(DATE_SUB(@date, INTERVAL 8 MONTH)))
            and u.property_id in ({property_id})
          group by
            upc.unit_id
            ) tupc on
      tupc.max_id = upc.id
          ) upc on
    upc.unit_id = u.id
  left join contact_leases cl on
    cl.lease_id = l.id
    and `primary` = 1
  left join contacts c on
    c.id = cl.contact_id
  left join unit_categories uc on
    uc.id = u.category_id
  where
    u.property_id in ({property_id})
    and l.status = 1
    and (LAST_DAY(DATE_SUB(@date, INTERVAL 8 MONTH)) >= l.start_date
      and (l.end_date is null
        or l.end_date > LAST_DAY(DATE_SUB(@date, INTERVAL 8 MONTH))))
  group by
    u.id
  UNION ALL
  select
    u.property_id,
    concat(if(isnull(p.number), '', concat(p.number, ' - ')), p.name) as property_name,
    u.status,
    u.id as unit_id,
    l.id as lease_id,
    u.category_id as unit_category_id,
    (
    select
      label
    from
      unit_types
    where
      id = u.unit_type_id) as unit_type,
    (
    select
      name
    from
      unit_types
    where
      id = u.unit_type_id) as unit_type_name,
    upc.id as unit_price_id,
    (cast(
          (
            select (
              ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
              * 
              ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
            )
            from units un where un.id = u.id
          )
        as decimal(10, 2)))
        as occupied_sqft,
    ifnull(upc.price, 0) as occupied_base_rent,
    ifnull(upc.set_rate, 0) as occupied_set_rate,
    ifnull(s.price, 0) as occupied_rent,
    TRIM(CONCAT(IFNULL(c.first, ''), ' ', IFNULL(c.last, ''))) as tenant_name,
    u.number,
    u.label,
    uc.description,
    LAST_DAY(DATE_SUB(@date, INTERVAL 9 MONTH)) as date
  from
    units u
  join properties p on
    p.id = u.property_id
  join leases l on
    l.unit_id = u.id
  left join services s on
    s.lease_id = l.id
    and s.product_id = u.product_id
    and s.status = 1
    and (LAST_DAY(DATE_SUB(@date, INTERVAL 9 MONTH)) >= s.start_date
      and (s.end_date is null
        or s.end_date >= LAST_DAY(DATE_SUB(@date, INTERVAL 9 MONTH))))
  left join (
    select
      upc.*
    from
      unit_price_changes upc
    join (
      select
        MAX(upc.id) as max_id
      from
        unit_price_changes upc
      join units u on
        u.id = upc.unit_id
      join properties p on
        p.id = u.property_id
      where
        DATE(CONVERT_TZ(upc.start, '+00:00', p.utc_offset )) <= LAST_DAY(DATE_SUB(@date, INTERVAL 9 MONTH))
          and (upc.end is null
            or DATE(CONVERT_TZ(upc.end, '+00:00', p.utc_offset )) > LAST_DAY(DATE_SUB(@date, INTERVAL 9 MONTH)))
            and u.property_id in ({property_id})
          group by
            upc.unit_id
            ) tupc on
      tupc.max_id = upc.id
          ) upc on
    upc.unit_id = u.id
  left join contact_leases cl on
    cl.lease_id = l.id
    and `primary` = 1
  left join contacts c on
    c.id = cl.contact_id
  left join unit_categories uc on
    uc.id = u.category_id
  where
    u.property_id in ({property_id})
    and l.status = 1
    and (LAST_DAY(DATE_SUB(@date, INTERVAL 9 MONTH)) >= l.start_date
      and (l.end_date is null
        or l.end_date > LAST_DAY(DATE_SUB(@date, INTERVAL 9 MONTH))))
  group by
    u.id
  UNION ALL
  select
    u.property_id,
    concat(if(isnull(p.number), '', concat(p.number, ' - ')), p.name) as property_name,
    u.status,
    u.id as unit_id,
    l.id as lease_id,
    u.category_id as unit_category_id,
    (
    select
      label
    from
      unit_types
    where
      id = u.unit_type_id) as unit_type,
    (
    select
      name
    from
      unit_types
    where
      id = u.unit_type_id) as unit_type_name,
    upc.id as unit_price_id,
    (cast(
          (
            select (
              ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
              * 
              ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
            )
            from units un where un.id = u.id
          )
        as decimal(10, 2)))
        as occupied_sqft,
    ifnull(upc.price, 0) as occupied_base_rent,
    ifnull(upc.set_rate, 0) as occupied_set_rate,
    ifnull(s.price, 0) as occupied_rent,
    TRIM(CONCAT(IFNULL(c.first, ''), ' ', IFNULL(c.last, ''))) as tenant_name,
    u.number,
    u.label,
    uc.description,
    LAST_DAY(DATE_SUB(@date, INTERVAL 10 MONTH)) as date
  from
    units u
  join properties p on
    p.id = u.property_id
  join leases l on
    l.unit_id = u.id
  left join services s on
    s.lease_id = l.id
    and s.product_id = u.product_id
    and s.status = 1
    and (LAST_DAY(DATE_SUB(@date, INTERVAL 10 MONTH)) >= s.start_date
      and (s.end_date is null
        or s.end_date >= LAST_DAY(DATE_SUB(@date, INTERVAL 10 MONTH))))
  left join (
    select
      upc.*
    from
      unit_price_changes upc
    join (
      select
        MAX(upc.id) as max_id
      from
        unit_price_changes upc
      join units u on
        u.id = upc.unit_id
      join properties p on
        p.id = u.property_id
      where
        DATE(CONVERT_TZ(upc.start, '+00:00', p.utc_offset )) <= LAST_DAY(DATE_SUB(@date, INTERVAL 10 MONTH))
          and (upc.end is null
            or DATE(CONVERT_TZ(upc.end, '+00:00', p.utc_offset )) > LAST_DAY(DATE_SUB(@date, INTERVAL 10 MONTH)))
            and u.property_id in ({property_id})
          group by
            upc.unit_id
            ) tupc on
      tupc.max_id = upc.id
          ) upc on
    upc.unit_id = u.id
  left join contact_leases cl on
    cl.lease_id = l.id
    and `primary` = 1
  left join contacts c on
    c.id = cl.contact_id
  left join unit_categories uc on
    uc.id = u.category_id
  where
    u.property_id in ({property_id})
    and l.status = 1
    and (LAST_DAY(DATE_SUB(@date, INTERVAL 10 MONTH)) >= l.start_date
      and (l.end_date is null
        or l.end_date > LAST_DAY(DATE_SUB(@date, INTERVAL 10 MONTH))))
  group by
    u.id
  UNION ALL
  select
    u.property_id,
    concat(if(isnull(p.number), '', concat(p.number, ' - ')), p.name) as property_name,
    u.status,
    u.id as unit_id,
    l.id as lease_id,
    u.category_id as unit_category_id,
    (
    select
      label
    from
      unit_types
    where
      id = u.unit_type_id) as unit_type,
    (
    select
      name
    from
      unit_types
    where
      id = u.unit_type_id) as unit_type_name,
    upc.id as unit_price_id,
    (cast(
          (
            select (
              ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
              * 
              ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
            )
            from units un where un.id = u.id
          )
        as decimal(10, 2)))
      as occupied_sqft,
    ifnull(upc.price, 0) as occupied_base_rent,
    ifnull(upc.set_rate, 0) as occupied_set_rate,
    ifnull(s.price, 0) as occupied_rent,
    TRIM(CONCAT(IFNULL(c.first, ''), ' ', IFNULL(c.last, ''))) as tenant_name,
    u.number,
    u.label,
    uc.description,
    LAST_DAY(DATE_SUB(@date, INTERVAL 11 MONTH)) as date
  from
    units u
  join properties p on
    p.id = u.property_id
  join leases l on
    l.unit_id = u.id
  left join services s on
    s.lease_id = l.id
    and s.product_id = u.product_id
    and s.status = 1
    and (LAST_DAY(DATE_SUB(@date, INTERVAL 11 MONTH)) >= s.start_date
      and (s.end_date is null
        or s.end_date >= LAST_DAY(DATE_SUB(@date, INTERVAL 11 MONTH))))
  left join (
    select
      upc.*
    from
      unit_price_changes upc
    join (
      select
        MAX(upc.id) as max_id
      from
        unit_price_changes upc
      join units u on
        u.id = upc.unit_id
      join properties p on
        p.id = u.property_id
      where
        DATE(CONVERT_TZ(upc.start, '+00:00', p.utc_offset )) <= LAST_DAY(DATE_SUB(@date, INTERVAL 11 MONTH))
          and (upc.end is null
            or DATE(CONVERT_TZ(upc.end, '+00:00', p.utc_offset )) > LAST_DAY(DATE_SUB(@date, INTERVAL 11 MONTH)))
            and u.property_id in ({property_id})
          group by
            upc.unit_id
            ) tupc on
      tupc.max_id = upc.id
          ) upc on
    upc.unit_id = u.id
  left join contact_leases cl on
    cl.lease_id = l.id
    and `primary` = 1
  left join contacts c on
    c.id = cl.contact_id
  left join unit_categories uc on
    uc.id = u.category_id
  where
    u.property_id in ({property_id})
    and l.status = 1
    and (LAST_DAY(DATE_SUB(@date, INTERVAL 11 MONTH)) >= l.start_date
      and (l.end_date is null
        or l.end_date > LAST_DAY(DATE_SUB(@date, INTERVAL 11 MONTH))))
  group by
    u.id
  UNION ALL
  select
    u.property_id,
    concat(if(isnull(p.number), '', concat(p.number, ' - ')), p.name) as property_name,
    u.status,
    u.id as unit_id,
    l.id as lease_id,
    u.category_id as unit_category_id,
    (
    select
      label
    from
      unit_types
    where
      id = u.unit_type_id) as unit_type,
    (
    select
      name
    from
      unit_types
    where
      id = u.unit_type_id) as unit_type_name,
    upc.id as unit_price_id,
    (cast(
          (
            select (
              ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'width' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
              * 
              ifnull((SELECT value from amenity_units where amenity_property_id = (select distinct ap.id from amenity_property ap where ap.property_id = un.property_id and ap.amenity_name = 'length' and ap.unit_type_id = un.unit_type_id and ap.deleted_at is null LIMIT 1) and unit_id = un.id LIMIT 1), 0)
            )
            from units un where un.id = u.id
          )
        as decimal(10, 2)))
        as occupied_sqft,
    ifnull(upc.price, 0) as occupied_base_rent,
    ifnull(upc.set_rate, 0) as occupied_set_rate,
    ifnull(s.price, 0) as occupied_rent,
    TRIM(CONCAT(IFNULL(c.first, ''), ' ', IFNULL(c.last, ''))) as tenant_name,
    u.number,
    u.label,
    uc.description,
    LAST_DAY(DATE_SUB(@date, INTERVAL 12 MONTH)) as date
  from
    units u
  join properties p on
    p.id = u.property_id
  join leases l on
    l.unit_id = u.id
  left join services s on
    s.lease_id = l.id
    and s.product_id = u.product_id
    and s.status = 1
    and (LAST_DAY(DATE_SUB(@date, INTERVAL 12 MONTH)) >= s.start_date
      and (s.end_date is null
        or s.end_date >= LAST_DAY(DATE_SUB(@date, INTERVAL 12 MONTH))))
  left join (
    select
      upc.*
    from
      unit_price_changes upc
    join (
      select
        MAX(upc.id) as max_id
      from
        unit_price_changes upc
      join units u on
        u.id = upc.unit_id
      join properties p on
        p.id = u.property_id
      where
        DATE(CONVERT_TZ(upc.start, '+00:00', p.utc_offset )) <= LAST_DAY(DATE_SUB(@date, INTERVAL 12 MONTH))
          and (upc.end is null
            or DATE(CONVERT_TZ(upc.end, '+00:00', p.utc_offset )) > LAST_DAY(DATE_SUB(@date, INTERVAL 12 MONTH)))
            and u.property_id in ({property_id})
          group by
            upc.unit_id
            ) tupc on
      tupc.max_id = upc.id
          ) upc on
    upc.unit_id = u.id
  left join contact_leases cl on
    cl.lease_id = l.id
    and `primary` = 1
  left join contacts c on
    c.id = cl.contact_id
  left join unit_categories uc on
    uc.id = u.category_id
  where
    u.property_id in ({property_id})
    and l.status = 1
    and (LAST_DAY(DATE_SUB(@date, INTERVAL 12 MONTH)) >= l.start_date
      and (l.end_date is null
        or l.end_date > LAST_DAY(DATE_SUB(@date, INTERVAL 12 MONTH))))
  group by
    u.id
      )
      SELECT property_id,
    DATE_FORMAT(ou.date, '%M-%y') as complimentary_month,
    count(ou.unit_id) as complimentary_count,
    sum(cast(ou.occupied_sqft as decimal(10, 2))) as complimentary_space_area
  FROM
    occupied_units ou
    where ou.occupied_rent = 0
  GROUP BY property_id, 
  DATE_FORMAT(ou.date, '%M-%y')
  ORDER BY property_id, 
  DATE_FORMAT(ou.date, '%M-%y');
     """
 
  redshift_df = fetch_data(redshift_engine, redshift_query)
  mysql_df = execute_mysql_statements(mysql_engine, mysql_query)
  # mysql_df['date'] = mysql_df['month'].apply(convert_to_last_day)

  # if isinstance(mysql_df, list):
    # You'll need the correct column names here
    # mysql_df = pd.DataFrame(mysql_df, columns=["property_id", "complimentary_month", "occupied_count", "occupied_space_area", "complimentary_count", "complimentary_space_area"])

  redshift_grouped = redshift_df
  mysql_grouped = mysql_df

  redshift_grouped['complimentary_month'] = redshift_grouped['complimentary_month'].str.replace(' ','',regex=False)

  # redshift_grouped.rename(columns={'date_dt':'date'},inplace=True)
  # redshift_grouped['date'] = pd.to_datetime(redshift_grouped['date'])
  # mysql_grouped['date'] = pd.to_datetime(mysql_grouped['date'])

  redshift_grouped = redshift_grouped.add_suffix('_tdw')
  mysql_grouped = mysql_grouped.add_suffix('_rds')

  # mysql_grouped = mysql_grouped.drop(columns=['occupied_count_rds','occupied_space_area_rds'])

  redshift_grouped.rename(columns={'property_id_tdw':'property_id','complimentary_month_tdw':'complimentary_month'},inplace=True)
  mysql_grouped.rename(columns={'property_id_rds':'property_id','complimentary_month_rds':'complimentary_month'},inplace=True)
  
  if redshift_df.empty and mysql_df.empty:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on = ['property_id','complimentary_month'], how='outer', indicator=True)
    return comparison_df
  else:
    comparison_df = pd.merge(redshift_grouped, mysql_grouped, on= ['property_id','complimentary_month'], how='outer', indicator=True)
    comparison_df['_merge'] = comparison_df['_merge'].map({'left_only': 'TDW', 'right_only': 'RDS', 'both': 'Both'})
    comparison_df['cm'] = pd.to_datetime(comparison_df['complimentary_month'],format='%B-%y')
    comparison_df = comparison_df.sort_values(['property_id','cm'],ascending=[True,False])
    comparison_df = comparison_df.drop(columns=['cm'])

    comparison_df.fillna({'complimentary_count_tdw': 0, 'complimentary_count_rds': 0,
                          'complimentary_space_area_tdw': 0,'complimentary_space_area_rds': 0}, inplace=True)
    # Determine the status
    comparison_df['Status'] = comparison_df.apply(
        lambda row: 'match' if row['complimentary_count_tdw'] == row['complimentary_count_rds'] 
        and row['complimentary_space_area_tdw'] == row['complimentary_space_area_rds']
        else 'mismatch', axis=1
    )
    
    return comparison_df



def main():
    tdw_username = os.environ["tdw_username"]
    tdw_password = os.environ["tdw_password"]
    tdw_port = os.environ["tdw_port"]
    tdw_db_endpoint = os.environ["tdw_database_endpoint"]
    tdw_db = os.environ["tdw_database"]

    postgre_username = os.environ["postgre_username"]
    postgre_password = os.environ["postgre_password"]
    postgre_endpoint = os.environ["postgre_database_endpoint"]
    postgre_db = os.environ["postgre_db"]

    rds_username = os.environ["rds_username"]
    rds_password = os.environ["rds_password"]
    rds_endpoint = os.environ["rds_database_endpoint"]
    rds_db = os.environ["rds_db"]


    print("username : ",tdw_username," password : ",tdw_password, "instance : ",tdw_db_endpoint)
    print("username : ",rds_username," password : ",rds_password, "instance : ",rds_endpoint)
    print("username : ",postgre_username," password : ",postgre_password, "instance : ",postgre_endpoint)
    redshift_engine = connect_redshift(tdw_username,tdw_password,tdw_port,tdw_db_endpoint,tdw_db)
    mysql_engine = connect_mysql(rds_username,rds_password,rds_endpoint,rds_db)
    postgre_engine = connect_postgresql(postgre_username,postgre_password,postgre_endpoint,postgre_db)


    if not redshift_engine or not mysql_engine or not postgre_engine:
        print("Error: Unable to establish database connections.")
        return
    
    print("Enter the date for RDS")
    rds_date = input()

    print("Enter the date for TDW")
    tdw_date = input()

    print("Enter refunds misc sk")
    tdw_refunds_misc_sk = input()

    temp_df = create_temp_table(redshift_engine)

    # print("Enter the property id of RDS")
    # rds_property_id = input()
    # print("Enter the property id of TDW")
    # tdw_property_id = input()

    # payment_metrics(redshift_engine,mysql_engine,tdw_date,rds_date,tdw_refunds_misc_sk)

    # df_rent_unchanged = rent_unchanged_in_past_year(redshift_engine,mysql_engine,tdw_date,rds_date)
    # df_rent_changed = rent_changed_in_past_year(redshift_engine,mysql_engine,tdw_date,rds_date)
    # df_credits_adjustments = credits_adjustments(redshift_engine,mysql_engine,tdw_date,rds_date)
    # df_payment_metrics = payment_metrics(redshift_engine,mysql_engine,tdw_date,rds_date,tdw_refunds_misc_sk)
    # df_refunds = refunds(redshift_engine,mysql_engine,tdw_date,rds_date,tdw_refunds_misc_sk)
    # df_move_in = Move_in(redshift_engine,mysql_engine,tdw_date,rds_date)
    # df_move_out = Move_out(redshift_engine,mysql_engine,tdw_date,rds_date)
    # df_delinquency = Delinquency(redshift_engine,mysql_engine,tdw_date,rds_date)
    # df_reservations = reservation(redshift_engine,mysql_engine,tdw_date,rds_date)
    df_autopay = autopay_enrollment(redshift_engine,mysql_engine,tdw_date,rds_date)
    # df_no_of_leases = number_of_leases(redshift_engine,mysql_engine,tdw_date,rds_date)
    # df_invoices_accural = invoices_accural_income(redshift_engine,mysql_engine,tdw_date,rds_date)
    # df_occupancy = Occupied_Units(redshift_engine,mysql_engine,tdw_date,rds_date)
    # df_complimetary = Complimentary_Units(redshift_engine,mysql_engine,tdw_date,rds_date)
    # df_prepaid_liabilities = Pre_paid_Liabilities(redshift_engine,mysql_engine,tdw_date,rds_date)
    # df_allowances_discounts = Allowances_Discounts(redshift_engine,mysql_engine,tdw_date,rds_date)
    # df_reserved = Reserved(redshift_engine,mysql_engine,tdw_date,rds_date)
    # df_user_reviews = User_Reviews(redshift_engine,postgre_engine,temp_df)
    # df_property_reviews = Property_Reviews(redshift_engine,postgre_engine,temp_df)
    # df_deposit_by_product = Deposits_by_Product(redshift_engine,mysql_engine,tdw_date,rds_date,tdw_refunds_misc_sk)
    # df_insurance_protection = Insurance_Protection_Enrollment(redshift_engine,mysql_engine,tdw_date,rds_date)
    # df_invoices = invoices(redshift_engine,mysql_engine,tdw_date,rds_date)
    # df_write_off = write_off(redshift_engine,mysql_engine,tdw_date,rds_date)
    # df_converted_leads = converted_leads(redshift_engine,mysql_engine,tdw_date,rds_date)
    # df_leads_activity = leads_activity(redshift_engine,mysql_engine,tdw_date,rds_date)
    # df_total_tasks = total_tasks_created_per_month(redshift_engine,mysql_engine,tdw_date,rds_date)
    # df_total_tasks_due = total_tasks_due_per_month(redshift_engine,mysql_engine,tdw_date,rds_date)
    # df_tasks_completed = total_tasks_completed(redshift_engine,mysql_engine)
    # df_pending_tasks = total_pending_tasks(redshift_engine,mysql_engine)
    # df_tasks_per_task_type = total_tasks_per_task_type(redshift_engine,mysql_engine)
    # df_tasks_completed_on_time = total_completed_tasks_on_time(redshift_engine,mysql_engine)
    # df_tasks_created_by_name = total_tasks_created_by_name(redshift_engine,mysql_engine)

    # print(df_user_reviews)


    with pd.ExcelWriter('Autopay_Sales_demo.xlsx', engine='openpyxl') as writer:
        # df_rent_unchanged.to_excel(writer, sheet_name='Rent_Unchanged', index=False)
        # df_rent_changed.to_excel(writer, sheet_name='Rent_Changed', index=False)
        # df_credits_adjustments.to_excel(writer, sheet_name='Credits_Adjustments', index=False)
        # df_payment_metrics.to_excel(writer, sheet_name='Payment_Metrics', index=False)
        # df_refunds.to_excel(writer, sheet_name='Refunds', index=False)
        # df_move_in.to_excel(writer, sheet_name='Move_In', index=False)
        # df_move_out.to_excel(writer, sheet_name='Move_Out', index=False)
        # df_delinquency.to_excel(writer, sheet_name='Delinquency', index=False)
        # df_reservations.to_excel(writer, sheet_name='Reservation', index=False)
        df_autopay.to_excel(writer, sheet_name='Autopay_Enrollment', index=False)
        # df_no_of_leases.to_excel(writer,sheet_name='Number_of_Leases',index=False)
        # df_invoices_accural.to_excel(writer,sheet_name='Invoices_Accural',index=False)
        # df_occupancy.to_excel(writer,sheet_name='Occupancy',index=False)
        # df_complimetary.to_excel(writer,sheet_name='Complimentary',index=False)
        # df_prepaid_liabilities.to_excel(writer,sheet_name='Prepaid_Liabilities',index=False)
        # df_allowances_discounts.to_excel(writer,sheet_name='Allowances_Discounts',index=False)
        # df_reserved.to_excel(writer, sheet_name='Reserved', index=False)
        # df_user_reviews.to_excel(writer, sheet_name='User_Reviews', index=False)
        # df_property_reviews.to_excel(writer, sheet_name='Property_Reviews', index=False)
        # df_deposit_by_product.to_excel(writer, sheet_name='Deposit_By_Product', index=False)
        # df_insurance_protection.to_excel(writer, sheet_name='Insurance_Protection', index=False)
        # df_total_tasks.to_excel(writer, sheet_name='total_tasks_created', index=False)
        # df_total_tasks_due.to_excel(writer, sheet_name='total_tasks_due', index=False)
        # df_tasks_completed.to_excel(writer, sheet_name='tasks_completed', index=False)
        # df_pending_tasks.to_excel(writer, sheet_name='pending_tasks', index=False)
        # df_tasks_per_task_type.to_excel(writer, sheet_name='tasks_per_task_type', index=False)
        # df_tasks_completed_on_time.to_excel(writer, sheet_name='tasks_completed_on_time', index=False)
        # df_tasks_created_by_name.to_excel(writer, sheet_name='tasks_created_by_name', index=False)
        # df_invoices.to_excel(writer, sheet_name='invoices', index=False)
        # df_write_off.to_excel(writer, sheet_name='write_off', index=False)
        # df_converted_leads.to_excel(writer, sheet_name='Converted_Leads', index=False)
        # df_leads_activity.to_excel(writer, sheet_name='Leads_Activity', index=False)

    #   df_total_tasks.to_excel(writer, sheet_name='total_tasks', index=False)
    #   df_tasks_completed.to_excel(writer, sheet_name='tasks_completed', index=False)
    #   df_pending_tasks.to_excel(writer, sheet_name='pending_tasks', index=False)
    #   df_tasks_per_task_type.to_excel(writer, sheet_name='tasks_per_task_type', index=False)
    #   df_tasks_completed_on_time.to_excel(writer, sheet_name='tasks_completed_on_time', index=False)
    #   df_tasks_created_by_name.to_excel(writer, sheet_name='tasks_created_by_name', index=False)

    print("finish code")
    
if __name__ == "__main__":
    main()
