import sys

import backoff
import custom_logger
import singer
from custom_logger import internal_logger, user_logger
from requests.exceptions import HTTPError

from tap_airtable.services import Airtable

_ = custom_logger

REQUIRED_CONFIG_KEYS = [
    "token",
]


class CustomException(Exception):
    pass


@backoff.on_exception(backoff.expo, CustomException, max_tries=3)
def operate(main_args):
    try:
        if main_args.discover:
            internal_logger.info("Discovery started")
            Airtable.run_discovery(main_args)
        elif main_args.properties:
            user_logger.info("Sync started")
            Airtable.run_sync(main_args.config, main_args.properties)
    except HTTPError as e:
        if e.response.status_code == 401:
            main_args.config = Airtable.refresh_token(main_args.config)
            raise CustomException()
        else:
            internal_logger.exception(e)
            sys.exit(-1)
    except Exception as e:
        internal_logger.exception(f"Error on execution: {e}")
        sys.exit(1)


def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    operate(args)


if __name__ == "__main__":
    main()
