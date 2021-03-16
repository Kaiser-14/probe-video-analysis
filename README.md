# Probe Video Analysis

## Introduction

The idea of this project is to analyse the content of specific video and provide a Quality Index metric. There is also availability to send information via Kafka bus or Rest API.

It has only be tested in Linux environments.

## Installation

### Development environment

#### Clone the repository
```bash
git clone https://github.com/Kaiser-14/probe-video-analysis.git
cd /probe-video-analysis/
```

#### Setup virtual environment (skip to install locally)
[Linux/Mac]
```bash
python3 -m venv venv
source /venv/bin/activate
```

[Windows]
```bash
\venv\Scripts\activate
```

#### Install dependencies
```bash
pip install -r requirements.txt
```

#### Download binaries
[Linux]
```bash
wget http://vq.kt.agh.edu.pl/metrics/mitsuLinuxMultithread
mv mitsuLinuxMultithread probe-video-analysis/mitsuMultithread
```
[Mac]
```bash
wget http://vq.kt.agh.edu.pl/metrics/mitsuMacOS
mv mitsuMacOS probe-video-analysis/mitsuMultithread
```
[Windows]
Unzip file and take a look at decompressed README file
```bash
wget http://vq.kt.agh.edu.pl/metrics/mitsuWin64_multithread.zip
[Unzip downloaded file]
move mitsuWin64_multithread.exe probe-video-analysis/mitsuMultithread
```

### Execution

There are many options to execute the program, check help manual.

```bash
python3 probe.py -h
```

#### Analyse and print locally Quality Index
Change input based on your actual video. It can be UDP streaming as in example, or local files. 
```bash
python3 probe.py -i [INPUT]
```

#### Analyse and send Quality Index via Kafka Bus
Provide IP and port from Kafka server, and also topic to produce messages
```bash
python3 probe.py -i [INPUT] -k [IP] [PORT] -t [TOPIC]
```

#### Analyse and send Quality Index via Rest API
Provide full Rest API path to send information
```bash
python3 probe.py -i [INPUT] -r [REST]
```