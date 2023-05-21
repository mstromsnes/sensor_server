import logging
from io import BytesIO
from typing import Optional

import httpx
import pandera as pa
from fastapi import HTTPException

from format import SerializationFormat, ParquetFormat, JSONFormat, SerializedDataFrame

logger = logging.getLogger("remotereader")


class ArchiveNotAvailableException(Exception):
    ...


def download_archive(
    url: str,
    format: SerializationFormat,
    timestamp: Optional[pa.DateTime] = None,
) -> SerializedDataFrame:
    try:
        response = send_request(url, timestamp)
    except httpx.TransportError:
        raise ArchiveNotAvailableException

    if response.status_code != 200:
        raise HTTPException(response.status_code, response.json())

    log_response(response)

    return extract_payload(response, format)


def send_request(url: str, timestamp: Optional[pa.DateTime]):
    if timestamp is None:
        response = httpx.get(url)
    else:
        response = httpx.post(url, json=str(timestamp))
    return response


def get_bytes_content(response: httpx.Response) -> BytesIO:
    buffer = BytesIO()
    buffer.write(response.content)
    buffer.seek(0)
    return buffer


def extract_payload(
    response: httpx.Response, format: SerializationFormat
) -> SerializedDataFrame:
    if isinstance(format, ParquetFormat):
        return get_bytes_content(response)
    elif isinstance(format, JSONFormat):
        return response.json()
    else:
        raise TypeError("Unsupported Format")


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
