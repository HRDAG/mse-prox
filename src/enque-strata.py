#!/usr/bin/env python3
# vim: set expandtab ts=4 sw=4 ai fileencoding=utf-8:
#
# Author: PB
# Maintainer(s): PB
# License: (c) HRDAG 2021, GPL v2 or newer
#
# -----------------------------------------------------------
# co-mse-prox/src/enque-strat.py
#

import sys

import boto3
from pathlib import Path
from glob import glob
import json
import time

import argparse
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s',
                    handlers=[logging.StreamHandler()])

def getargs():
    ipath = Path("/datos/compute/")
    parser = argparse.ArgumentParser()
    parser.add_argument("--strata", default=ipath / "strata")
    parser.add_argument("--estimates", default=ipath / "estimates")
    parser.add_argument("--max_chunks", default=0, type=int)
    args = parser.parse_args()
    return args


def get_strataq():
    sqs = boto3.client('sqs', region_name="us-east-2")
    squrl = [q for q in sqs.list_queues()['QueueUrls'] if 'strata' in q]
    assert len(squrl) == 1
    squrl = squrl[0]
    sqs = boto3.resource('sqs', region_name="us-east-2")
    return sqs.Queue(squrl)


def get_strata_paths(args):
    spaths = glob(str(args.strata / '*.json'))
    logging.info(f"found {len(spaths)} json files to enqueue")
    return spaths


def filter_spaths(spaths, args, verbose=False):
    strata_sha1s = set([str(f)[-45:-5] for f in spaths])
    assert all(len(s) == 40 for s in strata_sha1s)
    est_sha1s = set([str(f)[-45:-5] for f in
                 glob("/datos/estimates/fase3/**/*.json", recursive=True)])

    sha1s_to_estimate = strata_sha1s - est_sha1s

    if verbose:
        print(f"{len(spaths)} | {len(est_sha1s)} | {len(sha1s_to_estimate)}")
    spaths = [f"{args.strata}/{s}.json" for s in sha1s_to_estimate]
    assert all([Path(p).exists() for p in spaths])
    return spaths


def send_one_message(q, spath):
    sha = spath[-45:-5]
    with open(spath, 'rt') as f:
        js = f.read()
    msg = {'sha': sha, 'data': js}
    response = q.send_message(MessageBody = json.dumps(msg))
    assert int(response['ResponseMetadata']['HTTPStatusCode']) == 200
    return response


def send_n_messages(q, spaths):
    entries = list()
    assert len(spaths) <= 10
    for spath in spaths:
        sha = spath[-45:-5]
        with open(spath, 'rt') as f:
            js = f.read()
        msg = json.dumps({'sha': sha, 'data': js})
        entry = {'Id': str(sha),
                 'MessageBody': msg}
        entries.append(entry)
    response = q.send_messages(Entries=entries)
    return response


if __name__ == '__main__':
    strataq = get_strataq()
    args = getargs()
    spaths = get_strata_paths(args)
    spaths = filter_spaths(spaths, args)
    logging.info(f"after filter, found {len(spaths)} spaths to enqueue")

    maxitems = 2
    chunks = [spaths[x:x+maxitems] for x in range(0, len(spaths), maxitems)]
    if args.max_chunks != 0:
        chunks = chunks[0:args.max_chunks]
    logging.info(f"will add {len(chunks)} chunks of {maxitems} msgs")

    total_queued = 0
    for i, chunk in enumerate(chunks):
        response = send_n_messages(strataq, chunk)
        total_queued += len(chunk)
        if i % 100 == 0:
            logging.info(f"enqueued {total_queued} strata")

    logging.info(f"added {total_queued} to strataq; done.")

# done.
