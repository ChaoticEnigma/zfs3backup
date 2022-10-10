
import sys
import argparse
import logging

import boto3

from zfs3backup.config import get_config

log = logging.getLogger(__name__)


def download(bucket, name):
    try:
        bucket.download_fileobj(name, sys.stdout.buffer)
    except Exception as ex:
        print("Boto3 download_fileobj call failed",file=sys.stderr)
        print(ex)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Read a key from s3 and write the content to stdout",
    )
    parser.add_argument("name", help="name of S3 key")
    parser.add_argument("--verbose", "-v", dest="verbose", action="count",
                        help="Verbosity")
    return parser.parse_args()


def main():
    args = parse_args()

    if args.verbose:
        level = logging.DEBUG if args.verbose > 1 else logging.INFO
        logging.basicConfig(level=level, stream=sys.stderr, format="%(name)-20s %(levelname)8s -- %(message)s")

    cfg = get_config()

    profile = cfg.get("PROFILE")
    session = boto3.Session(profile_name=profile)

    endpoint = cfg.get("ENDPOINT")
    log.debug(f"Endpoint: {endpoint}")
    if endpoint == "aws":
        s3 = session.resource("s3")  # boto3.resource makes an intelligent decision with the default url
    else:
        s3 = session.resource("s3", endpoint_url=endpoint)

    bucket = s3.Bucket(cfg["BUCKET"])

    download(bucket, args.name)

    return 0


if __name__ == "__main__":
    sys.exit(main())
