#!/bin/bash

kill $(ps aux | grep 'python app.py' | awk '{print $2}')
python app.py &
