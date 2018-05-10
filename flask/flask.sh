#!/bin/bash

kill $(ps aux | grep 'python flask/fff.py' | awk '{print $2}')
python flask/fff.py &
