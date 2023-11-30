#!/usr/bin/env python

import json
import sys
from datetime import datetime

import paho.mqtt.client as mqtt
from rich.console import Console

import click

CLIENT_ID = "weewx-fetch"
TOPIC = "weather/loop"
OUTFILE_TXT = "./weewx-wx.txt"
OUTFILE_JSON = "./weewx-wx.json"

def on_connect(client, userdata, flags, rc):
    """The callback for when the client receives a CONNACK response from the server."""
    console = Console()
    console.print(f"Connected with result code {rc}")
    console.print(f"userdata: {userdata}")
    console.print(f"flags: {flags}")
    client.subscribe(TOPIC)
    console.print(f"Subscribed to {TOPIC}")


def on_message(client, userdata, msg):
    console = Console()
    #console.print(f"{msg.topic} {msg.payload}")
    wx_data = json.loads(msg.payload)
    #console.print(f"wx_data: {wx_data}")
    #console.print_json(data=wx_data)
    dt = datetime.fromtimestamp(float(wx_data['dateTime']))

    temperature = f"{float(wx_data['outTemp_F']):.1F}"
    humidity = f"{float(wx_data['outHumidity']):.0F}"
    dewpoint = f"{float(wx_data['dewpoint_F']):.1F}"
    wind_speed = f"{float(wx_data['windSpeed_mph']):.1f}"
    wind_dir = f"{float(wx_data.get('windDir', 0.00))}"
    pressure = "{float(wx_data['relbarometer']):.2f}"

    wx_str = (
        f"{dt:%Y-%m-%d %H:%M:%S}  "
        f"Temp: {temperature}F  "
        f"Humidity: {humidity}%  "
        f"Dewpoint: {dewpoint}F  "
        f"Wind: {wind_speed}mph@{wind_dir}  "
        f"Pressure: {pressure}inHg"
    )
    console.print(wx_str)

    wx_json = {
        'WX_TEMP': {
            "value": temperature,
            "format": "{:.1f}",
        } ,
        'WX_HUMIDITY': {
            "value": humidity,
            "format": "{:.0f}",
            },
        'WX_DEWPOINT': {
            "value": dewpoint,
            "format": "{:.1f}",
            },
        'WX_WIND_SPEED': {
            "value": wind_speed,
            "format": "{:.1f}",
            },
        'WX_WIND_DIR': {
            "value": wind_dir,
            "format": "{:.0f}",
            },
        'WX_PRESSURE': {
            "value": pressure,
            "format": "{:.2f}",
        }
    }
    console.print(wx_json)

    # Now write the temperature and dewpoint to a file
    with open(OUTFILE_TXT, "w") as f:
        f.write(wx_str)

    with open(OUTFILE_JSON, "w") as f:
        f.write(json.dumps(wx_json))


def on_disconnect(client, userdata, rc):
    console = Console()
    console.print(f"Disconnected with result code {rc}")
    console.print(f"userdata: {userdata}")


@click.command()
@click.option('--host', default="192.168.1.22",
              help='MQTT host.')
@click.option('--port', default=1883,
              help='MQTT Port')
@click.option('--username', default=None,
              help='MQTT username')
@click.option('--password', default=None,
              help='MQTT password')
def wx(host, port, username, password):
    """Fetch wx from weewx mqtt packet."""
    global client
    console = Console()

    client = mqtt.Client(
        client_id=CLIENT_ID,
        transport='tcp',
        protocol=mqtt.MQTTv311,
    )

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    if username:
        console.print(f"Using username {username}:{password}")
        client.username_pw_set(username=username, password=password)

    console.print(f"Connecting to {host}:{port}")
    client.connect(host, port, 60)
    console.print("Starting loop")
    client.loop_forever(timeout=60, max_packets=10)



if __name__ == '__main__':
    wx()
