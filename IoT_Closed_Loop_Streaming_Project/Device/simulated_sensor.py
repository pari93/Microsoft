import os
import time
import json
import random
from datetime import datetime, timezone
from azure.iot.device import IoTHubDeviceClient, MethodResponse

# Put your device connection string here (or use env var)
DEVICE_CONNECTION_STRING = os.getenv(
    "DEVICE_CONNECTION_STRING",
    "HostName=<your-hub>.azure-devices.net;DeviceId=temp-sensor-001;SharedAccessKey=<key>"
)

TARGET_TEMPERATURE = 24.0  # default target from "cloud"
CURRENT_TEMPERATURE = 24.0

def create_client():
    client = IoTHubDeviceClient.create_from_connection_string(DEVICE_CONNECTION_STRING)
    return client

def handle_twin_patch(patch):
    global TARGET_TEMPERATURE
    if "targetTemperature" in patch:
        TARGET_TEMPERATURE = patch["targetTemperature"]
        print(f"[DEVICE] New targetTemperature from cloud: {TARGET_TEMPERATURE}")

def handle_direct_method(request):
    global TARGET_TEMPERATURE
    if request.name == "setCoolingTarget":
        payload = request.payload  # e.g. {"targetTemperature": 21}
        TARGET_TEMPERATURE = payload.get("targetTemperature", TARGET_TEMPERATURE)
        print(f"[DEVICE] Direct method setCoolingTarget: {TARGET_TEMPERATURE}")
        response = MethodResponse.create_from_method_request(
            request, status=200, payload={"result": "ok"}
        )
    else:
        response = MethodResponse.create_from_method_request(
            request, status=404, payload={"result": "unknown method"}
        )
    return response

def main():
    global CURRENT_TEMPERATURE, TARGET_TEMPERATURE

    client = create_client()
    client.connect()

    # Subscribe to twin desired property updates
    client.on_twin_desired_properties_patch_received = handle_twin_patch

    # Subscribe to direct methods
    def on_method_request(request):
        response = handle_direct_method(request)
        client.send_method_response(response)

    client.on_method_request_received = on_method_request

    print("[DEVICE] Connected. Sending telemetry...")

    while True:
        # Simple physics: move current temperature slowly toward target
        CURRENT_TEMPERATURE += (TARGET_TEMPERATURE - CURRENT_TEMPERATURE) * 0.1

        telemetry = {
            "sensorId": "temp-sensor-001",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "temperature": round(CURRENT_TEMPERATURE, 2)
        }
        client.send_message(json.dumps(telemetry))
        print(f"[DEVICE] Telemetry sent: {telemetry}")

        # Report properties (so cloud sees current target)
        reported = {"targetTemperature": TARGET_TEMPERATURE}
        client.patch_twin_reported_properties(reported)

        time.sleep(5)

if __name__ == "__main__":
    main()