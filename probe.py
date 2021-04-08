import argparse
import datetime
import re
import subprocess
import sys
import time
import json
from uuid import getnode

import ffmpeg
import kafka
import pandas as pd
import requests
from kafka import KafkaProducer

parser = argparse.ArgumentParser(description='Multimedia probe analysis.')
parser.add_argument(
	'-i',
	dest='input',
	default='udp://localhost:1234',
	type=str,
	help='Multimedia input (could be streaming or local)')
parser.add_argument(
	'-k',
	dest='kafka',
	type=str,
	nargs=2,
	help='Kafka server (IP PORT)')
parser.add_argument(
	'-t',
	dest='topic',
	type=str,
	action='append',
	required='-k' in sys.argv,
	help='Kafka topic(s). Required if Kafka server provided')
parser.add_argument(
	'-r',
	dest='rest',
	type=str,
	help='Rest API server url (IP PORT)')

args = parser.parse_args()
mac_address = ':'.join(re.findall('..', '%012x' % getnode()))

if args.kafka:
	try:
		producer = KafkaProducer(bootstrap_servers=args.kafka[0] + ':' + args.kafka[1])
	except kafka.errors.NoBrokersAvailable as e:
		print('There are no Kafka brokers available in ' + args.kafka[0] + ':' + args.kafka[1])
		exit(0)

if args.rest:
	try:
		req = requests.get(args.rest)

		# Create metrics structure
		metrics = {
			'uuid': 'mac_address',
			'value': {
				'blockiness': None,
				'spatial_activity': None,
				'block_loss': None,
				'blur': None,
				'temporal_activity': None
			},
			'timestamp': datetime.datetime.now().timestamp()
		}

		# Create specific item if not present in server (409 code received if duplicated)
		if len(req.json()) > 0:
			for item in req.json():
				if item['uuid'] != mac_address:
					post = requests.post(args.rest, json=[metrics], headers={'Content-type': 'application/json'})
		else:
			post = requests.post(args.rest, json=[metrics], headers={'Content-type': 'application/json'})

	except requests.exceptions.ConnectionError:
		print('Probe could not reach REST API server')
		exit(0)

try:
	while True:
		# Convert stream to raw, saving the output locally
		try:
			stream = ffmpeg.input(args.input)

			stream = ffmpeg.output(
				stream, 'Multimedia/frame_ffmpeg.yuv', format='rawvideo', pix_fmt='yuv420p', t=4, framerate=60)
			ffmpeg.run(stream, overwrite_output=True, quiet=True)

		except ffmpeg.Error as e:
			time.sleep(1)
			# print('Retrying...')

		# Analyze raw video data
		text = subprocess.run([
			'./mitsuMultithread',
			'Multimedia/frame_ffmpeg.yuv',
			'1920',
			'1080'
		], capture_output=True, text=True).stdout

		# Convert received data from terminal to Dataframe, extracting only useful one
		df = pd.DataFrame(text.split('\n')[4:-4])

		data = []
		for i, row in df.iterrows():
			# First row (headers). Removing punctuations
			if i == 0:
				row_data = df[0][i].split('\t')
				for column in range(len(row_data)):
					row_data[column] = row_data[column].replace(':', "")
					row_data[column] = row_data[column].replace(' ', "")
				row_data = [x for x in row_data if x]
				data.append(row_data)

			# Data
			else:
				row_data = re.findall(r"(?i)\b[a-z-.0-9]+\b", df[0][i])
				data.append(row_data)

		# Dump data into new dataframe
		df = pd.DataFrame(data)
		df = df.rename(columns=df.iloc[0])  # Set first row (headers) as columns
		df = df.drop(df.index[0])
		df = df.astype(float)

		# Manipulate data
		blockiness = df['Blockiness'].iloc[-60:].mean()
		spatial_activity = df['SpatialActivity'].iloc[-60:].mean()
		block_loss = df['Blockloss'].iloc[-60:].mean()
		blur = df['Blur'].iloc[-60:].mean()
		temporal_activity = df['TemporalAct'].iloc[-60:].mean()

		# Convert data into JSON
		metrics = {
			'uuid': mac_address,
			'value': {
				'blockiness': blockiness,
				'spatial_activity': spatial_activity,
				'block_loss': block_loss,
				'blur': blur,
				'temporal_activity': temporal_activity
			},
			'timestamp': datetime.datetime.now().timestamp()
		}

		# Print information locally
		if not args.kafka and not args.rest:
			print(metrics)

		# Send information via Kafka bus
		if args.kafka:
			# Every metric condensed into one message
			if len(args.topic) == 1:
				producer.send(args.topic[0], json.dumps(metrics).encode('utf-8'))
			# Publish metrics individually
			else:
				producer.send(args.topic[0], blockiness)
				producer.send(args.topic[1], spatial_activity)
				producer.send(args.topic[2], block_loss)
				producer.send(args.topic[3], blur)
				producer.send(args.topic[4], temporal_activity)
			print(metrics)
			print('Metrics published into Kafka bus')

		# Send information via Rest API
		if args.rest:
			req = requests.put(args.rest, json=[metrics], headers={'Content-type': 'application/json'})
			print(metrics)
			if req.status_code:
				print('Metrics sent via REST API')
				print('---------------')
			else:
				print('Not possible to send metrics via REST API')

except KeyboardInterrupt:
	print('Received Keyboard Interrupt. Shutting down.')
	exit(0)
