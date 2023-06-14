import logging
from io import BytesIO
from typing import Optional

import httpx
import pandera as pa
from fastapi import HTTPException

from format import Format

ARCHIVE_ENDPOINT = "/archive/"

logger = logging.getLogger("remotereader")


class ArchiveNotAvailableException(Exception):
    ...


def download_archive(
    url: str, timestamp: Optional[pa.DateTime] = None, format: Format = Format.Parquet
):
    try:
        response = send_request(url, timestamp, format)
    except httpx.TransportError:
        raise ArchiveNotAvailableException

    if response.status_code != 200:
        raise HTTPException(response.status_code, response.json())

    log_response(response)

    return extract_payload(response, format), format


def send_request(
    url: str, timestamp: Optional[pa.DateTime], format: Format = Format.Parquet
):
    format_endpoint = format.endpoint()
    request_url = url + ARCHIVE_ENDPOINT + format_endpoint
    if timestamp is None:
        response = httpx.get(request_url, timeout=30)
    else:
        response = httpx.post(request_url, json=str(timestamp))
    return response


def get_bytes_content(response: httpx.Response) -> BytesIO:
    buffer = BytesIO()
    buffer.write(response.content)
    buffer.seek(0)
    return buffer


def extract_payload(response: httpx.Response, format):
    if format is Format.Parquet:
        return get_bytes_content(response)
    if format is Format.JSON:
        return response.json()


def register_as_forwarding_server(url: str):
    request_url = url + "/register_forwarding_server/"
    return httpx.post(request_url, json={"port": "8005"})


def remove_as_forwarding_server(url: str):
    request_url = url + "/register_forwarding_server/"
    return httpx.delete(request_url)


def log_response(response: httpx.Response):
    size = response.headers["content-length"]
    logging.info(f"{format=} Response Size={size}")
    logging.debug(f"{response.status_code=}")
