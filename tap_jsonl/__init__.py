#!/usr/bin/env python3

from xmlrpc.client import boolean
import singer
import argparse
import json
import os
import copy
from singer import Transformer
import jsonlines

REQUIRED_CONFIG_KEYS = ["files"]
STATE = {}
CONFIG = {}

logger = singer.get_logger()


def to_singer_schema(input):
    if type(input) == dict:
        property = dict(type=["object", "null"], properties={})
        for k, v in input.items():
            property["properties"][k] = to_singer_schema(v)
        return property
    elif type(input) == list:
        if len(input):
            new_input = {}
            for i in input:
                new_input.update(i)
            return dict(type=["array", "null"], items=to_singer_schema(new_input))
        else:
            return {"type": ["array", "null"]}
    elif type(input) == boolean:
        return {"type": ["boolean", "null"]}
    elif type(input) == int:
        return {"type": ["integer", "null"]}
    elif type(input) == float:
        return {"type": ["number", "null"]}
    return {"type": ["string", "null"]}


def process_file(fileInfo):
    # determines if file in question is a file or directory and processes accordingly
    if not os.path.exists(fileInfo["file"]):
        logger.warning(fileInfo["file"] + " does not exist, skipping")
        return
    if os.path.isdir(fileInfo["file"]):
        fileInfo["file"] = (
            os.path.normpath(fileInfo["file"]) + os.sep
        )  # ensures directories end with trailing slash
        logger.info(
            "Syncing all JSONL files in directory '"
            + fileInfo["file"]
            + "' recursively"
        )
        for filename in os.listdir(fileInfo["file"]):
            subInfo = copy.deepcopy(fileInfo)
            subInfo["file"] = fileInfo["file"] + filename
            process_file(subInfo)  # recursive call
    else:
        sync_file(fileInfo)


def sync_file(fileInfo):
    if fileInfo["file"].split(".")[-1] not in ["jsonl", "json"]:
        logger.warning("Skipping non-jsonl file '" + fileInfo["file"] + "'")
        logger.warning(
            "Please provide a jsonl file that ends with '.jsonl' or 'json'; e.g. 'users.jsonl'"
        )
        return

    logger.info(
        f"Syncing entity {fileInfo['entity']} from file: {fileInfo['file']}"
    )
    with jsonlines.open(fileInfo["file"]) as reader:
        needsHeader = True
        for row in reader:
            if needsHeader:
                header_map = to_singer_schema(row)
                singer.write_schema(
                    fileInfo["entity"], header_map, fileInfo.get("keys", [])
                )
                needsHeader = False
                
            with Transformer() as transformer:
                rec = transformer.transform(row, header_map)
                singer.write_record(fileInfo["entity"], rec)
    singer.write_state(STATE)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Config file", required=False)
    parser.add_argument("-s", "--state", help="State file", required=False)
    parser.add_argument(
        "-p", "--properties", help="Property selections", required=False
    )
    parser.add_argument("--catalog", help="Catalog file", required=False)
    parser.add_argument(
        "-d",
        "--discover",
        action="store_true",
        help="Do schema discovery",
        required=False,
    )
    return parser.parse_args()


def load_json(path):
    with open(path) as f:
        return json.load(f)


def check_config(config, required_keys):
    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        logger.error(
            "tap-jsonl: Config is missing required keys: {}".format(missing_keys)
        )
        exit(1)


def do_sync():
    logger.info("Starting sync")

    check_config(CONFIG, REQUIRED_CONFIG_KEYS)
    jsonl_files = CONFIG["files"]

    for fileInfo in jsonl_files:
        process_file(fileInfo)
    logger.info("Sync completed")


def main():
    args = parse_args()

    if args.discover:
        catalog = {"streams": []}
        print(json.dumps(catalog, indent=2))
    elif not args.config:
        logger.error("tap-jsonl: the following arguments are required: -c/--config")
        exit(1)
    else:
        config = load_json(args.config)

        if args.state:
            state = load_json(args.state)
        else:
            state = {}

        CONFIG.update(config)
        STATE.update(state)
        do_sync()


if __name__ == "__main__":
    main()
