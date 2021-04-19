import argparse
import datetime
import json
import os
import re
import subprocess
import sys
import time
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
			'uuid': mac_address,
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
		if mac_address not in list(item['uuid'] for item in req.json()):
			requests.post(args.rest, json=[metrics], headers={'Content-type': 'application/json'})

	except requests.exceptions.ConnectionError:
		print('Probe can not reach REST API server')
		exit(0)

try:
	while True:
		# Convert stream to raw, saving the output locally
		try:
			stream = ffmpeg.input(args.input)

			print('Obtaining data...')
			stream = ffmpeg.output(
				stream, 'frames_ffmpeg.yuv', format='rawvideo', pix_fmt='yuv420p', t=4, framerate=60)
			ffmpeg.run(stream, overwrite_output=True, quiet=True)

		except ffmpeg.Error as e:
			time.sleep(1)
			# print('Retrying...')

		# Analyze raw video data
		print('Analyzing data...')
		text = subprocess.run([
			'./mitsuMultithread',
			'frames_ffmpeg.yuv',
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
			print('Metrics:')
			print(metrics)
			print('---------------')

		# Send information via Kafka bus
		if args.kafka:
			# Every metric condensed into one message
			if len(args.topic) == 1:
				producer.send(args.topic[0], json.dumps(metrics).encode('utf-8'))
			# Publish metrics individually
			else:
				producer.send(args.topic[0], str(blockiness).encode('utf-8'))
				producer.send(args.topic[1], str(spatial_activity).encode('utf-8'))
				producer.send(args.topic[2], str(block_loss).encode('utf-8'))
				producer.send(args.topic[3], str(blur).encode('utf-8'))
				producer.send(args.topic[4], str(temporal_activity).encode('utf-8'))
			print('Metrics published into Kafka bus:')
			print(metrics)
			print('---------------')

		# Send information via Rest API
		if args.rest:

			try:
				req = requests.put(args.rest, json=[metrics], headers={'Content-type': 'application/json'})
			except:
				req.status_code = 404
			# Update metrics with PUT message. If it fails (e.g. instance deleted from API server), try to create the
			# instance and resend metrics.
			if req.status_code == 200:
				print('Metrics sent via REST API:')
			else:
				try:
					req = requests.post(args.rest, json=[metrics], headers={'Content-type': 'application/json'})
				except:
					req.status_code = 404

				if req.status_code == 201:
					print('Metrics sent via REST API:')
				else:
					print('Could not send messages. Check API server.')
			print(metrics)
			print('---------------')

except KeyboardInterrupt:
	print('Received Keyboard Interrupt. Shutting down.')
	try:
		os.remove('frames_ffmpeg.yuv')
		os.remove('metricsResultsCSV.csv')
	except FileNotFoundError:
		pass
	exit(0)
