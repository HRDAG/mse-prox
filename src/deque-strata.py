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

import sys
import boto3
from pathlib import Path
import json
import yaml
import time


def get_strataq():
    sqs = boto3.client('sqs', region_name="us-east-2")
    squrl = [q for q in sqs.list_queues()['QueueUrls'] if 'strata' in q]
    assert len(squrl) == 1
    squrl = squrl[0]
    sqs = boto3.resource('sqs', region_name="us-east-2")
    return sqs.Queue(squrl)


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


if __name__ == '__main__':
    strataq = get_strataq()

    msgid, receipt, sha, data = get_1msg(strataq)
    if msgid is None:
        sys.exit(0)

    input_sha = f"input/{sha}.json"
    with open(input_sha, 'wt') as f:
        f.write(data)

    yaml_sha = f"output/{sha}.yaml"
    with open(yaml_sha, 'wt') as f:
        yaml.dump({'msgid': msgid, 'receipt': receipt,
                   'start_time': time.time()}, f)

    sys.stdout.write(f"{sha}\n")
# done.
