"""Consume session events, enrich them, and route by customer country."""
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
parser.add_argument("--source_stream", required=True, help="Source Kinesis stream")
parser.add_argument(
    "--dest_streams",
    required=True,
    help='JSON map with "USA" and "International" destination stream names',
)


def serialize_datetime(value):
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    raise TypeError(f"Unsupported JSON type: {type(value).__name__}")


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


def transform_session(session):
    session["processing_timestamp"] = datetime.datetime.now(datetime.timezone.utc)
    products = session.get("browse_history", [])
    session["overall_product_quantity"] = sum(int(p["quantity"]) for p in products)
    session["overall_in_shopping_cart"] = sum(
        int(p["quantity"]) for p in products if p["in_shopping_cart"] is True
    )
    session["total_different_products"] = len(products)
    return session


def poll_shards(client, pairs, destinations):
    while True:
        for pair in pairs:
            try:
                response = client.get_records(ShardIterator=pair.iterator, Limit=200)
                for record in response.get("Records", []):
                    session = json.loads(record["Data"].decode("utf-8"))
                    try:
                        enriched = transform_session(session)
                        destination = (
                            destinations["USA"]
                            if enriched.get("country") == "USA"
                            else destinations["International"]
                        )
                        result = client.put_record(
                            StreamName=destination,
                            Data=json.dumps(enriched, default=serialize_datetime).encode("utf-8"),
                            PartitionKey=str(enriched["session_id"]),
                        )
                        logging.info(
                            "Processed session %s and wrote to %s (shard %s)",
                            enriched["session_id"],
                            destination,
                            result["ShardId"],
                        )
                    except Exception:
                        logging.exception("Could not transform or route a session")
                pair.iterator = response.get("NextShardIterator", pair.iterator)
            except Exception:
                logging.exception("Failed reading source shard %s", pair.shard_id)
        time.sleep(2)


def main():
    args = parser.parse_args()
    destinations = json.loads(args.dest_streams)
    if set(destinations) != {"USA", "International"}:
        raise ValueError("dest_streams must contain exactly USA and International")
    client = boto3.client("kinesis")
    pairs = fetch_shards_and_iterators(client, args.source_stream)
    logging.info("Starting ETL consumer for %d shard(s)", len(pairs))
    poll_shards(client, pairs, destinations)


if __name__ == "__main__":
    main()
