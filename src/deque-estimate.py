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


def get_estimateq():
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


if __name__ == '__main__':
    estimateq = get_estimateq()

    msgid, receipt, sha, data = get_1msg(estimateq)
    if msgid is None:
        sys.exit(1)

    prefix = sha[0:2]
    # write to /datos/estimates/fase3/prefix/
    outputfile = Path("/datos/estimates/fase3/") / f"{prefix}/{sha}.json"
    with open(outputfile, 'wt') as f:
        f.write(data)

    yaml_sha = f"output/{sha}.yaml"
    with open(yaml_sha, 'wt') as f:
        yaml.dump({'msgid': msgid, 'receipt': receipt}, f)

    sys.stdout.write(f"{sha}\n")
# done.
