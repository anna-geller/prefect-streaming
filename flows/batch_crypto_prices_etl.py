from prefect import task, flow, get_run_logger
from datetime import datetime
import requests
import pandas as pd
import awswrangler as wr


@task
def extract_current_prices():
    url = "https://min-api.cryptocompare.com/data/pricemulti?fsyms=BTC,ETH,REP,DASH&tsyms=USD"
    response = requests.get(url)
    prices = response.json()
    logger = get_run_logger()
    logger.info("Received data: %s", prices)
    return prices


@task
def transform_current_prices(json_data: dict) -> pd.DataFrame:
    df = pd.DataFrame(json_data)
    now = datetime.utcnow()
    logger = get_run_logger()
    logger.info("Adding a column TIME with current time: %s", now)
    df["TIME"] = now
    return df.reset_index(drop=True)


@task
def load_current_prices(df: pd.DataFrame):
    table_name = "crypto"
    wr.s3.to_parquet(
        df=df,
        path="s3://prefectdata/crypto/",
        dataset=True,
        mode="append",
        database="default",
        table=table_name,
    )
    logger = get_run_logger()
    logger.info("Table %s in Athena data lake successfully updated 🚀", table_name)


@flow
def crypto_prices_etl():
    # Real-time ETL
    raw_prices = extract_current_prices()
    transformed_data = transform_current_prices(raw_prices)
    load_current_prices(transformed_data)


if __name__ == "__main__":
    crypto_prices_etl()
