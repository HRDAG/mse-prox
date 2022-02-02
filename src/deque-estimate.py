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
import datetime
from collections import deque
import glob


elapsed_times = deque()


# TODO: add imput from enque-strata or functions to refactor these fns
def get_strata_paths():
    spaths = glob.glob('/datos/compute/strata/*.json')
    print(f"found {len(spaths)} json files total")
    return spaths


def filter_spaths(spaths, verbose=False):
    strata_sha1s = set([str(f)[-45:-5] for f in spaths])
    assert all(len(s) == 40 for s in strata_sha1s)
    est_sha1s = set([str(f)[-45:-5] for f in
                 glob.glob("/datos/estimates/fase3/**/*.json", recursive=True)])

    sha1s_to_estimate = strata_sha1s - est_sha1s

    if verbose:
        print(f"{len(spaths)} | {len(est_sha1s)} | {len(sha1s_to_estimate)} remaining")
    spaths = [f"/datos/compute/strata/{s}.json" for s in sha1s_to_estimate]
    assert all([Path(p).exists() for p in spaths])
    return len(sha1s_to_estimate)


def mtime():
    nows = datetime.datetime.now()
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
    # print(f"found {num_messages}, processing")
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
    print(f"{mtime()}: retrieved {len(entries)}, w estimatesQ={num_estimates}; "
          f"in strataQ={num_strata}; ")
    return True


if __name__ == '__main__':
    estimatesq = get_estimatesq()
    strataq = get_strataq()
    last_time = datetime.datetime.now() - datetime.timedelta(minutes=10)
    prev_num_remaining = 100000

    while True:
        status = get_5msgs(estimatesq, strataq)
        if datetime.datetime.now() - last_time > datetime.timedelta(minutes=1):
            spaths = get_strata_paths()
            num_remaining = filter_spaths(spaths, verbose=False)
            num_done = prev_num_remaining - num_remaining
            prev_num_remaining = num_remaining
            secs_elapsed = (datetime.datetime.now() - last_time).total_seconds()
            last_time = datetime.datetime.now()

            if num_done > 0:
                secs_per_stratum = secs_elapsed / num_done
                est_time_remaining = round((secs_per_stratum * num_remaining) / 60, 1)
            else:
                est_time_remaining = "NA"
            print(f"done in last {round(secs_elapsed, 0)}s: {num_done}; "
                  f"strata remaining={num_remaining}; "
                  f"est time remaining={est_time_remaining} mins")
        if not status:
            print(f"{mtime()} no estimates found, sleeping 1m")
            time.sleep(60)


# done.
