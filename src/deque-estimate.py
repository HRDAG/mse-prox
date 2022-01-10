#!/usr/bin/env python3
# vim: set expandtab ts=4 sw=4 ai fileencoding=utf-8:
#
# Author: PB
# Maintainer(s): PB
# License: (c) HRDAG 2021, GPL v2 or newer
#
# -----------------------------------------------------------
# deque-strata.py
#

import numpy as np
import sys
import boto3
from pathlib import Path
import json
import time
import yaml
from datetime import datetime
from collections import deque


elapsed_times = deque()


def mtime():
    nows = datetime.now()
    return nows.strftime('%Y-%m-%dT%H:%M:%S%z')

def get_strataq():
    sqs = boto3.client('sqs', region_name="us-east-2")
    squrl = [q for q in sqs.list_queues()['QueueUrls'] if 'strata' in q]
    assert len(squrl) == 1
    squrl = squrl[0]
    sqs = boto3.resource('sqs', region_name="us-east-2")
    return sqs.Queue(squrl)


def get_estimatesq():
    sqs = boto3.client('sqs', region_name="us-east-2")
    qurl = [q for q in sqs.list_queues()['QueueUrls'] if 'estimates' in q]
    assert len(qurl) == 1
    qurl = qurl[0]
    sqs = boto3.resource('sqs', region_name="us-east-2")
    return sqs.Queue(qurl)


def get_1msg(q):
    response = q.receive_messages(
        MaxNumberOfMessages=1,
        WaitTimeSeconds=20,
        AttributeNames=['All'],
    )
    num_messages = len(response)
    if num_messages == 0:
        return None, None, None, None
    assert num_messages == 1
    message = response[0]

    body = json.loads(message.body)
    sha = body['sha']
    data = body['data']

    receipt = message.receipt_handle
    msgid = message.message_id
    return msgid, receipt, sha, data


def get_5msgs(estimatesq, strataq):
    response = estimatesq.receive_messages(
        MaxNumberOfMessages=5,
        WaitTimeSeconds=20,
        AttributeNames=['All'],
    )
    num_messages = len(response)
    print(f"found {num_messages}, processing")
    if num_messages == 0:
        return False

    entries = list()
    for message in response:

        body = json.loads(message.body)
        sha = body['sha']
        data = body['data']

        elapsed = body.get('elapsed', -1)
        if elapsed not in [-1, 'NA']:
            elapsed_times.appendleft(elapsed)
        if len(elapsed_times) > 100:
            elapsed_times.pop()

        receipt = message.receipt_handle
        msgid = message.message_id

        prefix = sha[0:2]
        outputdir = Path("/datos/estimates/fase3/") / f"{prefix}"
        outputdir.mkdir(exist_ok=True)
        outputpath = outputdir / f"{sha}.json"
        if outputpath.exists():
            print(f"hit existing sha1 {sha}, skipping")
        else:
            outputpath.write_text(data)

        entries.append({'Id': msgid, 'ReceiptHandle': receipt})

    response = estimatesq.delete_messages(Entries=entries)
    if not 'Successful' in response:
        print(response)
        sys.exit(1)

    if len(elapsed_times) > 5:
        mn_elapsed = int(np.mean(elapsed_times))
    else:
        mn_elapsed = "NA"
    num_estimates = estimatesq.attributes['ApproximateNumberOfMessages']
    num_strata = strataq.attributes['ApproximateNumberOfMessages']
    print(f"{mtime()} ; in estimatesQ={num_estimates}; in strataQ={num_strata}; "
          f"mean time per estimate: {mn_elapsed}")
    return True


if __name__ == '__main__':
    estimatesq = get_estimatesq()
    strataq = get_strataq()

    while True:
        status = get_5msgs(estimatesq, strataq)
        if not status:
            print(f"{mtime()} no estimates found, sleeping 1m")
            time.sleep(60)


# done.
