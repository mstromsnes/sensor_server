from fastapi.testclient import TestClient
from httpx import Response
from server import app, get_datastore, get_forwarder, get_publisher
from sensor import SensorReading
from datastore import SensorData, DataStore
from publisher import Publisher
from forwarding import ForwardingManager
from pathlib import Path
from datetime import datetime, timedelta
from remotereader import get_bytes_content
import pandas as pd

ARCHIVE_PATH = Path("archive/test.parquet")
TEST_DATASTORE = DataStore(parquet_file=ARCHIVE_PATH)
TEST_PUBLISHER = Publisher()
TEST_FORWARDER = ForwardingManager()


def get_datastore_test():
    return TEST_DATASTORE


def get_publisher_test():
    return TEST_PUBLISHER


def get_forwarder_test():
    return TEST_FORWARDER


client = TestClient(app)

app.dependency_overrides[get_datastore] = get_datastore_test
app.dependency_overrides[get_publisher] = get_publisher_test
app.dependency_overrides[get_forwarder] = get_forwarder_test


def parquet_response_to_df(
    response: Response,
) -> pd.DataFrame:
    df = pd.read_parquet(get_bytes_content(response))
    SensorData.validate(df)
    return df


def test_hello_world():
    response = client.get("/helloworld/")
    assert response.status_code == 200
    assert response.text == "Hello World!"


def test_archive_is_valid_dataframe():
    response = client.get("/archive/parquet/")
    assert response.status_code == 200
    df = parquet_response_to_df(response)


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
    df = parquet_response_to_df(response)
    assert df.size != 0
