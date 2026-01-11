#!/bin/sh
python -m venv ./venv
pip install -r requirements.txt
docker-compose up -d
sleep 5
python producer.py
