#!/bin/bash

micromamba activate || true

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "$(dirname $SCRIPT_DIR)"

mkdir input || true
mkdir output || true

git pull || true

for (( i=1; i<=$1; i++ ))
do
	src/estimate.sh &
done
