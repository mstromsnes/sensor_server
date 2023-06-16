from datetime import datetime

import pandas as pd
import pytest
from fastapi.testclient import TestClient
from httpx import Response

from remotereader import get_bytes_content
from sensordata import SensorData
from server import app, get_datastore, get_forwarder, get_publisher


@pytest.fixture
def read_only_client(read_only_datastore, publisher, forwarder) -> TestClient:
    app.dependency_overrides[get_datastore] = lambda: read_only_datastore
    app.dependency_overrides[get_publisher] = lambda: publisher
    app.dependency_overrides[get_forwarder] = lambda: forwarder

    return TestClient(app)


def parquet_response_to_df(
    response: Response,
) -> pd.DataFrame:
    df = SensorData.Parquet.load(get_bytes_content(response))
    SensorData.Model.validate(df)
    return df


def test_hello_world(read_only_client: TestClient):
    response = read_only_client.get("/helloworld/")
    assert response.status_code == 200
    assert response.text == "Hello World!"


def test_archive_is_valid_dataframe(read_only_client: TestClient):
    response = read_only_client.get("/archive/parquet/")
    assert response.status_code == 200
    df = parquet_response_to_df(response)


def test_end_of_data_collection_returns_empty_dataframe(read_only_client: TestClient):
    timestamp = datetime(2024, 5, 17)
    response = read_only_client.post("/archive/json/", json=str(timestamp))
    assert response.status_code == 200
    df = pd.read_json(response.json(), orient="table")
    SensorData.Model.validate(df)
    assert df.size == 0


def test_start_of_data_collection_returns_nonempty_dataframe(
    read_only_client: TestClient,
):
    timestamp = datetime(2023, 5, 17)
    response = read_only_client.post("/archive/parquet/", json=str(timestamp))
    assert response.status_code == 200
    df = parquet_response_to_df(response)
    assert df.size != 0
