import logging
import uvicorn
from fastapi import FastAPI
from sensor import SensorReading

app = FastAPI()


@app.post("/")
def add_reading(reading: SensorReading):
    logging.log(logging.INFO, reading)
    return reading


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
