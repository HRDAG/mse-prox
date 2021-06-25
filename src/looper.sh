#!/bin/bash

for (( i=1; i<=$1; i++ ))
do
	src/estimate.sh &
done
