import awswrangler as wr
from datetime import datetime
import os
import pandas as pd
import requests
from typing import Dict

from prefect import task, flow, get_run_logger
from prefect_slack import SlackWebhook
from prefect_slack.messages import send_incoming_webhook_message


@task
def extract_current_prices() -> Dict[str, Dict[str, float]]:
    url = "https://min-api.cryptocompare.com/data/pricemulti?fsyms=BTC,ETH,REP,DASH&tsyms=USD"
    response = requests.get(url)
    prices = response.json()
    logger = get_run_logger()
    logger.info("Received data: %s", prices)
    return prices


@task
def transform_current_prices(json_data: Dict[str, Dict[str, float]]) -> pd.DataFrame:
    df = pd.DataFrame(json_data)
    now = datetime.utcnow()
    logger = get_run_logger()
    logger.info("Adding a column TIME with current time: %s", now)
    df["TIME"] = now
    return df.reset_index(drop=True)


@task
def load_current_prices(df: pd.DataFrame) -> None:
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
def crypto_prices_etl(price_threshold: int = 18_000) -> None:
    raw_prices = extract_current_prices()
    curr_price = raw_prices.result().get("BTC").get("USD")
    if curr_price < price_threshold:
        logger = get_run_logger()
        logger.info(
            "Price = %s below threshold = %d! Sending alert to buy",
            curr_price,
            price_threshold,
        )
        send_incoming_webhook_message(
            slack_webhook=SlackWebhook(os.environ["SLACK_WEBHOOK_URL"]),
            text=f"BTC price is lower than threshold ({curr_price}), time to buy! :tada:",
            wait_for=[raw_prices],
        )
    data = transform_current_prices(raw_prices)
    load_current_prices(data)


if __name__ == "__main__":
    while True:
        crypto_prices_etl()
