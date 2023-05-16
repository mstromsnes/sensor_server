from fastapi.testclient import TestClient
from server import app
from sensor import SensorReading
from datastore import SensorData
from datetime import datetime, timedelta
from remotereader import get_bytes_content
import pandas as pd

client = TestClient(app)


def test_hello_world():
    response = client.get("/helloworld/")
    assert response.status_code == 200
    assert response.text == "Hello World!"


def test_archive_is_valid_dataframe():
    response = client.get("/archive/parquet/")
    assert response.status_code == 200
    df = pd.read_parquet(get_bytes_content(response))
    SensorData.validate(df)


def test_future_returns_empty_dataframe():
    timestamp = datetime.now() + timedelta(365)
    response = client.post("/archive/json/", json=str(timestamp))
    assert response.status_code == 200
    df = pd.read_json(response.json(), orient="table")
    SensorData.validate(df)
    assert df.size == 0


def test_past_returns_nonempty_dataframe():
    timestamp = datetime.now() - timedelta(365)
    response = client.post("/archive/parquet/", json=str(timestamp))
    assert response.status_code == 200
    df = pd.read_parquet(get_bytes_content(response))
    SensorData.validate(df)
    assert df.size != 0
