import pandas as pd

def extract_orders(csv_path):
    df=pd.read_csv(csv_path)
    return df

def transform_orders(df):

    df['order_date']=pd.to_datetime(df['order_date'],errors='coerce')

    df=df[df['amount']>0]

    conversion_rate={
        'USD':1,
        'INR':0.012
    }

    df['amount_usd']=df.apply(
        lambda row:row['amount']*conversion_rate[row['currency']],
        axis=1
    )

    df=df[['order_id','customer_name','order_date','amount_usd']]

    df=df.dropna(subset=["order_date"])

    return df
