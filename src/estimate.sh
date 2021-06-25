#!/bin/bash
# vim: set expandtab ts=4:
#
# Authors:	   PB
# Maintainers: PB
# Copyright:   2021, HRDAG, GPL v2 or later
# =========================================
# co-mse-prox/src/estimate.sh

# assure execution from just above this script's dir
# https://stackoverflow.com/questions/59895/how-can-i-get-the-source-directory-of-a-bash-script-from-within-the-script-itsel
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "$(dirname $SCRIPT_DIR)"

mkdir input || true
mkdir output || true

while true;
do

	sha=$(python3 src/deque-strata.py)

	if [[ -n $sha ]];
	then
		nice -n5 Rscript --vanilla src/estimate.R $sha && \
			python3 src/enque-estimate.py $sha && \
			echo "proxd $sha"
	else
		echo "error! no sha received. sleeping"
		sleep 1m
	fi

done

# done.

