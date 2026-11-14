"""Continuously read records from each shard of a Kinesis Data Stream."""
import argparse
import json
import logging
import sys
import time

import boto3

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    handlers=[logging.FileHandler("consumer.log"), logging.StreamHandler(sys.stdout)],
)

parser = argparse.ArgumentParser()
parser.add_argument("--stream", required=True, help="Kinesis stream name")


class ShardIteratorPair:
    def __init__(self, shard_id, iterator):
        self.shard_id = shard_id
        self.iterator = iterator


def fetch_shards_and_iterators(client, stream_name):
    pairs = []
    response = client.list_shards(StreamName=stream_name)
    while True:
        for shard in response.get("Shards", []):
            shard_id = shard["ShardId"]
            result = client.get_shard_iterator(
                StreamName=stream_name,
                ShardId=shard_id,
                ShardIteratorType="TRIM_HORIZON",
            )
            pairs.append(ShardIteratorPair(shard_id, result["ShardIterator"]))
        token = response.get("NextToken")
        if not token:
            break
        response = client.list_shards(StreamName=stream_name, NextToken=token)
    return pairs


def poll_shards(client, pairs):
    while True:
        for pair in pairs:
            try:
                response = client.get_records(ShardIterator=pair.iterator, Limit=200)
                for record in response.get("Records", []):
                    event = json.loads(record["Data"].decode("utf-8"))
                    logging.info(
                        "Read event from shard %s, sequence %s: %s",
                        pair.shard_id,
                        record["SequenceNumber"],
                        event,
                    )
                pair.iterator = response.get("NextShardIterator", pair.iterator)
            except Exception:
                logging.exception("Failed reading shard %s", pair.shard_id)
        time.sleep(1)


def main():
    args = parser.parse_args()
    client = boto3.client("kinesis")
    pairs = fetch_shards_and_iterators(client, args.stream)
    logging.info("Starting consumer for %d shard(s)", len(pairs))
    poll_shards(client, pairs)


if __name__ == "__main__":
    main()
