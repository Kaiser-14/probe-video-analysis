import ffmpeg
import subprocess
import time
import pandas as pd
import re
import argparse
import sys
import kafka
from kafka import KafkaProducer
import requests

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

if args.kafka:
	try:
		producer = KafkaProducer(bootstrap_servers=args.kafka[0] + ':' + args.kafka[1])
		exit(0)
	except kafka.errors.NoBrokersAvailable as e:
		print('There are no Kafka brokers available in ' + args.kafka[0] + ':' + args.kafka[1])
		exit(0)

if args.rest:
	try:
		req = requests.get(args.rest)
		exit(0)
	except requests.exceptions.ConnectionError:
		print('Probe could not reach Rest API server')
		exit(0)

while True:
	# Convert stream to raw, saving the output locally
	try:
		# stream = ffmpeg.input('Multimedia/game_10.mkv', t=3)
		stream = ffmpeg.input(args.input)
		# stream = ffmpeg.input('udp://localhost:1234')
		stream = ffmpeg.output(stream, 'Multimedia/frame_ffmpeg.yuv', format='rawvideo', pix_fmt='yuv420p', t=4, framerate=60)
		out, error = ffmpeg.run(stream, overwrite_output=True, capture_stdout=True, capture_stderr=True, quiet=True)
		# print('FFmpeg error: '.format(error))
		# print(out)
	except ffmpeg.Error as e:
		# print('stdout:', e.stdout.decode('utf8'))
		# print('stderr:', e.stderr.decode('utf8'))
		# raise e
		time.sleep(1)

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
	# print(df['Blockiness'].iloc[-60:].mean())
	blockiness = df['Blockiness'].iloc[-60:].mean()

	# print(df['Blockloss'].iloc[-60:].mean())
	blockloss = df['Blockloss'].iloc[-60:].mean()

	# print(df['Blur'].iloc[-60:].mean())
	blur = df['Blur'].iloc[-60:].mean()

	qi = (blur * blockiness) / blockloss

	# Print information locally
	if not args.kafka and not args.rest:
		print(qi)

	# Send information via Kafka bus
	if args.kafka:
		# TODO: Send corresponding metric
		producer.send(args.topic, qi)

	# Send information via Rest API
	if args.rest:
		req = requests.put(args.rest, qi)

	# Handle process
	input("Press Enter to continue...")