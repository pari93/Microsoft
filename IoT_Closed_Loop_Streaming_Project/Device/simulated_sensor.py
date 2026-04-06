import os
import time
import json
import random
from datetime import datetime, timezone
from azure.iot.device import IoTHubDeviceClient, MethodResponse
from dotenv import load_dotenv
load_dotenv()

# Put your device connection string here (or use env var)
DEVICE_CONNECTION_STRING = os.getenv("DEVICE_CONNECTION_STRING")

TARGET_TEMPERATURE = 35.0  # Desired temperature
CURRENT_TEMPERATURE = 35.0 # Current temperature of the device [Change to >28 to see cooling in action or <18 to see heating in action immediately in the terminal logs]

# Create a client that can send telemetry and receive twin updates
def create_client():
    client = IoTHubDeviceClient.create_from_connection_string(DEVICE_CONNECTION_STRING)
    return client

# After receiving the new temperature set device's internal target temperature
def handle_twin_patch(patch):
    global TARGET_TEMPERATURE

    # Ignore patches that don't contain targetTemperature
    if "targetTemperature" not in patch:
        return

    new_value = patch["targetTemperature"]

    # Ignore repeated values
    if new_value == TARGET_TEMPERATURE:
        return

    TARGET_TEMPERATURE = new_value
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

    #Connect device to IoT Hub
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

        # 1. If no targetTemperature has been received yet → hold steady
        if TARGET_TEMPERATURE is None:
            # Natural tiny drift only
            CURRENT_TEMPERATURE += random.uniform(-0.5, 1.2)

        # 2. If a targetTemperature has been received → move toward it
        else:
            CURRENT_TEMPERATURE += (TARGET_TEMPERATURE - CURRENT_TEMPERATURE) * 0.1  # Adjust the 0.1 factor to control how quickly it moves toward the target

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

        time.sleep(60)

if __name__ == "__main__":
    main()