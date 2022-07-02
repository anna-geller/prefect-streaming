FROM prefecthq/prefect:2.0b7-python3.9
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY flows/ .
CMD ["python", "crypto_prices_etl.py"]