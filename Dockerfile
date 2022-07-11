FROM prefecthq/prefect:2.0b8-python3.9
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY realtime-poc/ .
CMD ["python", "real_time_flow.py"]