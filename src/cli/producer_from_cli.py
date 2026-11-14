"""Send one JSON session record to an Amazon Kinesis Data Stream."""
import argparse
import json
import logging
import sys

import boto3

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    handlers=[logging.FileHandler("producer.log"), logging.StreamHandler(sys.stdout)],
)

parser = argparse.ArgumentParser(description="Produce a JSON session record")
parser.add_argument("--stream", required=True, help="Kinesis stream name")
parser.add_argument("--json_string", required=True, help="JSON event payload")


def main():
    args = parser.parse_args()
    record = json.loads(args.json_string)
    response = boto3.client("kinesis").put_record(
        StreamName=args.stream,
        Data=json.dumps(record).encode("utf-8"),
        PartitionKey=str(record["session_id"]),
    )
    logging.info(
        "Produced record %s to shard %s",
        response["SequenceNumber"],
        response["ShardId"],
    )


if __name__ == "__main__":
    main()
