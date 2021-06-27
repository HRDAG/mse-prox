#!/usr/bin/env python3
# vim: set expandtab ts=4 sw=4 ai fileencoding=utf-8:
#
# Author: PB
# Maintainer(s): PB
# License: (c) HRDAG 2021, GPL v2 or newer
#
# -----------------------------------------------------------
# co-mse-prox/src/enque-estimate.py
#

import sys

import boto3
from pathlib import Path
from glob import glob
import json
import yaml
import time

import argparse

def getargs():
    parser = argparse.ArgumentParser()
    parser.add_argument("sha")
    args = parser.parse_args()
    return args


def get_estimatesq():
    sqs = boto3.client('sqs', region_name="us-east-2")
    qurl = [q for q in sqs.list_queues()['QueueUrls'] if 'estimates' in q]
    assert len(qurl) == 1
    qurl = qurl[0]
    sqs = boto3.resource('sqs', region_name="us-east-2")
    return sqs.Queue(qurl)


def get_strataq():
    sqs = boto3.client('sqs', region_name="us-east-2")
    squrl = [q for q in sqs.list_queues()['QueueUrls'] if 'strata' in q]
    assert len(squrl) == 1
    squrl = squrl[0]
    sqs = boto3.resource('sqs', region_name="us-east-2")
    return sqs.Queue(squrl)


def send_one_message(q, sha, elapsed, spath):
    with open(spath, 'rt') as f:
        js = f.read()
    msg = {'sha': sha, 'data': js, 'elapsed': elapsed}
    response = q.send_message(MessageBody = json.dumps(msg))
    assert int(response['ResponseMetadata']['HTTPStatusCode']) == 200
    return response


if __name__ == '__main__':
    strataq = get_strataq()
    estimatesq = get_estimatesq()
    args = getargs()

    inputfile = Path("input") / f"{args.sha}.json"
    yfile = Path("output") / f"{args.sha}.yaml"
    msginfo = yaml.safe_load(open(yfile, 'rt'))
    if 'start_time' in msginfo:
        elapsed_time = int(time.time() - float(msginfo['start_time']))
    else:
        elapsed_time = 'NA'

    # send output to estimates queue
    outputfile = Path("output") / f"{args.sha}.json"
    send_one_message(estimatesq, args.sha, elapsed_time, outputfile)

    response = strataq.delete_messages(Entries=[{
        'Id': msginfo['msgid'],
        'ReceiptHandle': msginfo['receipt']}])

    if not 'Successful' in response:
        print(f"warning: failed to delete message from strata queue")

    inputfile.unlink()
    yfile.unlink()
    outputfile.unlink()
# done.
