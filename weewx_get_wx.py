#!/usr/bin/env python

import json
import sys
from datetime import datetime

import paho.mqtt.client as mqtt
from rich.console import Console

import click

TOPIC = "weather/loop"

#OUTFILE = "/home/waboring/allsky/tmp/weewx-wx.txt"
OUTFILE = "./weewx-wx.txt"

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

    wx_str = (
        f"{dt:%Y-%m-%d %H:%M:%S}  "
        f"Temp: {float(wx_data['outTemp_F']):.1f}F  "
        f"Humidity: {float(wx_data['outHumidity']):.0f}%  "
        f"Dewpoint: {float(wx_data['dewpoint_F']):.1f}F  "
        f"Wind: {float(wx_data['windSpeed_mph']):.1f}mph@{float(wx_data.get('windDir', 0.00))}  "
        f"Pressure: {float(wx_data['relbarometer']):.2f}inHg"
    )
    console.print(wx_str)

    # Now write the temperature and dewpoint to a file
    with open(OUTFILE, "w") as f:
        f.write(wx_str)


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
        client_id="allsky-fetch",
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
    #client.loop(timeout=60, max_packets=10)
    client.loop_forever(timeout=60, max_packets=10)



if __name__ == '__main__':
    wx()
