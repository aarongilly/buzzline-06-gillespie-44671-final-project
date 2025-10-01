# NW Missouri State University CSIS 44671 Final Module

Date: 2025-09-30
Author: Aaron Gillespie
GitHub: https://github.com/aarongilly

## About 

This repo is the final project for Northwest Missouri State University CSIS 44671 - Streaming Data. It will take place over the course of a couple weeks, which as of this sentence I am just at the start of.

> [!NOTE] TL;DR
> This is a demo project featuring Kafka, Data producer (reading CSV as though it were a stream), data cosumers, and auto-updating visualizations.

This project will be focused around **my own personal biometric data** collected from my [Oura Ring](https://ouraring.com/) over the past ~4 years. I am leveraging data from some pre-existing (outside of this repo) work I've done, using [Google Apps Script](https://developers.google.com/apps-script) with [Oura's API](https://cloud.ouraring.com/v2/docs) to pull my own data to a Google Sheet.

![Overview Image](assets/44671_Final_Project_Overview.excalidraw.png)

Code from the left half of this diagram is not contained within this repo.

### Pipeline

![pipeline](assets/pipeline.png)

The CSV file is standing in for some sort of on-going, continual data generator. In practice, the `producer` would likely utilize some sort of API (Twitter, Reddit) or in situ data source (manufacturing machine, health monitor). For the sake of simplicity we will substitute a CSV utilizing data that *very well could have* come from a data stream - data from my Oura Ring. The `consumer` is operating like a typical consumer would, although its functionality is (intentionally) minimal.

## Running

Running the code necessitates 3 terminals, run simultaneously. Pull up 3 terminals and run the following 3 commands on each in order:

1. start kafka

```bash
chmod +x scripts/prepare_kafka.sh
scripts/prepare_kafka.sh
cd ~/kafka
bin/kafka-server-start.sh config/kraft/server.properties
```

2. start the producer

```zsh
source .venv/bin/activate
python3 -m producers.producer_gillespie
```

3. start the consumer

```zsh
source .venv/bin/activate
python3 -m consumers.consumer_gillespie
```


