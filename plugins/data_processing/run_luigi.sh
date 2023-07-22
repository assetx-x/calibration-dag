#!/bin/bash

MODULE=${1%.*}
PIPELINE=${1##*.} 

LUIGI_SCHEDULER=luigi-scheduler.marathon.mesos

source activate dcm-intuition
while true; do luigi --module $MODULE $PIPELINE --scheduler-host=$LUIGI_SCHEDULER && sleep 600; done

