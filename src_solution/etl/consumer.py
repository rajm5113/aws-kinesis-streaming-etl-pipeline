"""Lab reference implementation of the streaming ETL consumer.

This file is kept separate from the main implementation for comparison.
"""
import argparse
import datetime
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
parser.add_argument("--source_stream", required=True)
parser.add_argument("--dest_streams", required=True)


def serialize_datetime(value):
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    raise TypeError("Type not serializable")


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
            iterator = client.get_shard_iterator(
                StreamName=stream_name,
                ShardId=shard_id,
                ShardIteratorType="TRIM_HORIZON",
            )["ShardIterator"]
            pairs.append(ShardIteratorPair(shard_id, iterator))
        token = response.get("NextToken")
        if not token:
            break
        response = client.list_shards(StreamName=stream_name, NextToken=token)
    return pairs


def poll_shards(client, pairs, destinations):
    while True:
        for pair in pairs:
            try:
                response = client.get_records(ShardIterator=pair.iterator, Limit=200)
                for record in response.get("Records", []):
                    session = json.loads(record["Data"].decode("utf-8"))
                    try:
                        session["processing_timestamp"] = datetime.datetime.now()
                        overall_product_quantity = 0
                        overall_in_shopping_cart = 0
                        for product in session["browse_history"]:
                            overall_product_quantity += int(product["quantity"])
                            if product["in_shopping_cart"] is True:
                                overall_in_shopping_cart += int(product["quantity"])
                        session["overall_product_quantity"] = overall_product_quantity
                        session["overall_in_shopping_cart"] = overall_in_shopping_cart
                        session["total_different_products"] = len(session["browse_history"])
                        destination = destinations["USA"] if session["country"] == "USA" else destinations["International"]
                        result = client.put_record(
                            StreamName=destination,
                            Data=json.dumps(session, default=serialize_datetime).encode("utf-8"),
                            PartitionKey=session["session_id"],
                        )
                        logging.info("Processed session %s to shard %s", session["session_id"], result["ShardId"])
                    except Exception:
                        logging.exception("Error transforming or forwarding the record")
                pair.iterator = response.get("NextShardIterator", pair.iterator)
            except Exception:
                logging.exception("Failed fetching records from shard %s", pair.shard_id)
        time.sleep(2)


def main():
    args = parser.parse_args()
    destinations = json.loads(args.dest_streams)
    client = boto3.client("kinesis")
    pairs = fetch_shards_and_iterators(client, args.source_stream)
    poll_shards(client, pairs, destinations)


if __name__ == "__main__":
    main()
