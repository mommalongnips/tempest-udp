import os
import socket
import json
import logging
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from .env file
load_dotenv()

# Setup InfluxDB client for InfluxDB 2.0
def influxdb_client():
    logging.info("Setting up InfluxDB client")
    return InfluxDBClient(
        url=os.getenv("INFLUXDB_URL"),
        token=os.getenv("INFLUXDB_TOKEN"),
        org=os.getenv("INFLUXDB_ORG")
    )

# Parse the obs_st message into a Point for InfluxDB 2.0
def parse_obs_st_message(data):
    logging.debug(f"Received data: {data}")
    
    json_data = json.loads(data)
    
    if json_data.get("type") != "obs_st":
        logging.info(f"Ignored message with type: {json_data.get('type')}")
        return None
    
    obs = json_data["obs"][0]
    point = Point("obs_st") \
        .tag("device_id", json_data["serial_number"]) \
        .tag("hub_sn", json_data["hub_sn"]) \
        .tag("firmware_revision", json_data["firmware_revision"]) \
        .field("time_epoch_seconds", obs[0]) \
        .field("wind_lull_mps", obs[1]) \
        .field("wind_avg_mps", obs[2]) \
        .field("wind_gust_mps", obs[3]) \
        .field("wind_direction_degrees", obs[4]) \
        .field("wind_sample_interval_seconds", obs[5]) \
        .field("station_pressure_mb", obs[6]) \
        .field("air_temperature_celsius", obs[7]) \
        .field("relative_humidity_percent", obs[8]) \
        .field("illuminance_lux", obs[9]) \
        .field("uv_index", obs[10]) \
        .field("solar_radiation_wm2", obs[11]) \
        .field("rain_accumulated_mm", obs[12]) \
        .field("precipitation_type", obs[13]) \
        .field("lightning_strike_avg_distance_km", obs[14]) \
        .field("lightning_strike_count", obs[15]) \
        .field("battery_voltage_v", obs[16]) \
        .field("report_interval_minutes", obs[17])
    
    logging.debug(f"Constructed Point: {point}")
    return point

# Listen for obs_st messages on the network and write them to InfluxDB
def listen_for_obs_st():
    logging.info("Starting UDP listener")
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.bind(('', 50222))
    
    client = influxdb_client()
    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    while True:
        try:
            data, addr = sock.recvfrom(1024)
            logging.debug(f"Received packet from {addr}")
            point = parse_obs_st_message(data.decode('utf-8'))
            if point:
                write_api.write(bucket=os.getenv("INFLUXDB_BUCKET"), org=os.getenv("INFLUXDB_ORG"), record=point)
                logging.info(f"Data written to InfluxDB: {point}")
        except Exception as e:
            logging.error(f"Error processing packet: {e}")

if __name__ == "__main__":
    try:
        logging.info("Starting the application")
        listen_for_obs_st()
    except KeyboardInterrupt:
        logging.info("Terminated by user")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")