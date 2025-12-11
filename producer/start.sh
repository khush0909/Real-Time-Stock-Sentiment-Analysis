#!/bin/bash

# Start the news producer in the background
python -u producer.py &

# Start the price producer in the foreground
python -u price_producer.py
