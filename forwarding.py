import httpx

from sensor import SensorReading


class ForwardingManager:
    def __init__(self):
        self.forwarding_endpoints: list[str] = list()

    def register_forwarding_endpoint(self, endpoint):
        self.forwarding_endpoints.append(endpoint)

    def remove_forwarding_endpoint(self, endpoint):
        self.forwarding_endpoints.remove(endpoint)

    def broadcast(self, message: SensorReading):
        for endpoint in self.forwarding_endpoints:
            httpx.post(endpoint, json=message.json())
