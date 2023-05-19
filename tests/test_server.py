from httpx import Response
from datastore import SensorData
from fastapi.testclient import TestClient
from datetime import datetime, timedelta
from format import Format
from remotereader import get_bytes_content
import pandas as pd


def parquet_response_to_df(
    response: Response,
) -> pd.DataFrame:
    df = Format.Parquet.load(get_bytes_content(response))
    SensorData.validate(df)
    return df


def test_hello_world(client: TestClient):
    response = client.get("/helloworld/")
    assert response.status_code == 200
    assert response.text == "Hello World!"


def test_archive_is_valid_dataframe(client: TestClient):
    response = client.get("/archive/parquet/")
    assert response.status_code == 200
    df = parquet_response_to_df(response)


def test_future_returns_empty_dataframe(client: TestClient):
    timestamp = datetime.now() + timedelta(365)
    response = client.post("/archive/json/", json=str(timestamp))
    assert response.status_code == 200
    df = pd.read_json(response.json(), orient="table")
    SensorData.validate(df)
    assert df.size == 0


def test_past_returns_nonempty_dataframe(client: TestClient):
    timestamp = datetime.now() - timedelta(365)
    response = client.post("/archive/parquet/", json=str(timestamp))
    assert response.status_code == 200
    df = parquet_response_to_df(response)
    assert df.size != 0
