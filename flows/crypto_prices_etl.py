import boto3
import os
from prefect import task, flow, get_run_logger
from datetime import datetime
import requests
import pandas as pd
import awswrangler as wr
from prefect_slack import SlackWebhook
from prefect_slack.messages import send_incoming_webhook_message


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


@task
def get_price_threshold():
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("crypto")
    response = table.get_item(
        Key={
            "coin": "btc",
        }
    )
    thresh_value = response["Item"]["threshold"]
    logger = get_run_logger()
    logger.info("Current BTC threshold: %s", thresh_value)
    return float(thresh_value)


@flow(name="crypto_prices_etl")
def crypto_prices_etl():
    # Real-time ETL
    raw_prices = extract_current_prices()
    transformed_data = transform_current_prices(raw_prices)
    load_current_prices(transformed_data)
    # Taking action in real-time
    thresh_future = get_price_threshold()
    thresh_value = thresh_future.result()
    curr_price = raw_prices.result().get("BTC").get("USD")
    logger = get_run_logger()
    if curr_price < thresh_value:
        logger.info(
            "Price (%s) is below threshold (%d)! Sending alert!",
            curr_price,
            thresh_value,
        )
        send_incoming_webhook_message(
            slack_webhook=SlackWebhook(os.environ["SLACK_WEBHOOK_URL"]),
            text=f"BTC price ({curr_price}) is lower than threshold ({thresh_value})!",
            wait_for=[raw_prices, thresh_future],
        )
    else:
        logger.info("Current price (%d) is too high. Skipping alert", curr_price)


if __name__ == "__main__":
    while True:
        crypto_prices_etl()
