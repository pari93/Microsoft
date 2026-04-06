import datetime
import logging
import os
from urllib import response

import azure.functions as func

from azure.kusto.data import KustoConnectionStringBuilder, KustoClient
from azure.iot.hub import IoTHubRegistryManager
from azure.identity import DefaultAzureCredential
from azure.digitaltwins.core import DigitalTwinsClient


app = func.FunctionApp()

# Schedule format: {second} {minute} {hour} {day} {month} {day-of-week}
@app.schedule(schedule="0 */5 * * * *", arg_name="mytimer", run_on_startup=False, use_monitor=False) # Every 5 minutes
def ClosedLoopController(mytimer: func.TimerRequest) -> None:
    logging.info("ClosedLoopController triggered")

    # ----------------------------------------------------------------------
    # 1. Read configuration from environment variables
    # ----------------------------------------------------------------------
    ADX_CLUSTER = os.getenv("ADX_CLUSTER_URI")
    ADX_DB = os.getenv("ADX_DATABASE")
    IOTHUB_CONN = os.getenv("IOTHUB_CONNECTION_STRING")
    ADT_URL = os.getenv("ADT_URL")

    DEVICE_ID = "temp-sensor-001"

    # ----------------------------------------------------------------------
    # 2. Query ADX for last 5 minutes of temperature data
    # ----------------------------------------------------------------------
    try:
        kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(ADX_CLUSTER)
        client = KustoClient(kcsb)

        query = """
        TemperatureReadings
        | where timestamp > ago(3m)
        | summarize avg_temp = avg(temperature)
        """

        response = client.execute(ADX_DB, query)

        # Extract the first row
        table = response.primary_results[0]
        rows = table.rows
        if len(rows) == 0:
            logging.warning("No telemetry found in ADX for the last 3 minutes.")
            return

        row = rows[0]
        avg_temp = row["avg_temp"]

        logging.info(f"Average temperature from ADX: {avg_temp}")

    except Exception as ex:
        logging.error(f"ADX query failed: {ex}")
        return

    if avg_temp is None or str(avg_temp).lower() == "nan":
        logging.warning("No valid temperature data available yet. Waiting for telemetry...")
        return

    # ----------------------------------------------------------------------
    # 3. Decide control action
    # ----------------------------------------------------------------------
    COOLING_THRESHOLD = 28.0
    HEATING_THRESHOLD = 18.0

    # Handle NaN or missing data
    if avg_temp is None or str(avg_temp).lower() == "nan":
        logging.warning("No valid temperature data available yet. Waiting for telemetry...")
        return

    if avg_temp > COOLING_THRESHOLD:
        new_mode = "COOLING_ON"
        target_temp = 22
    elif avg_temp < HEATING_THRESHOLD:
        new_mode = "HEATING_ON"
        target_temp = 24
    else:
        new_mode = "IDLE"
        target_temp = None

    logging.info(f"Control decision: {new_mode}")

    # ----------------------------------------------------------------------
    # 4. Update IoT Hub Device Twin
    # ----------------------------------------------------------------------
    try:
        registry = IoTHubRegistryManager(IOTHUB_CONN)

        twin_patch = {
            "properties": {
                "desired": {
                    "mode": new_mode,
                    "targetTemperature": target_temp
                }
            }
        }

        registry.update_twin(DEVICE_ID, twin_patch, "*")
        logging.info(f"Updated IoT Hub twin for {DEVICE_ID}")

    except Exception as ex:
        logging.error(f"Failed to update IoT Hub twin: {ex}")

    # ----------------------------------------------------------------------
    # 5. Update Azure Digital Twins
    # ----------------------------------------------------------------------
    try:
        credential = DefaultAzureCredential()
        dt_client = DigitalTwinsClient(ADT_URL, credential)

        patch = [
            {"op": "replace", "path": "/mode", "value": new_mode}
        ]

        if target_temp is not None:
            patch.append({
                "op": "replace",
                "path": "/targetTemperature",
                "value": target_temp
            })

        dt_client.update_digital_twin(DEVICE_ID, patch)
        logging.info(f"Updated ADT twin for {DEVICE_ID}")

    except Exception as ex:
        logging.error(f"Failed to update ADT: {ex}")

    logging.info("Closed-loop cycle complete.")