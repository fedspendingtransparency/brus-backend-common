import argparse
import logging

from brus_backend_common.models import DELTA_MODELS
from brus_backend_common.helpers.spark_helper import SparkScriptSession

logger = logging.getLogger(__name__)


def setup_parser(parser):
    """Separating parser functionality as USAS uses Django Commands"""
    parser.add_argument(
        "--table",
        "-t",
        type=str,
        required=True,
        help="The destination Delta Table to write the data",
        choices=list(DELTA_MODELS.keys()),
    )
    behavior = parser.add_mutually_exclusive_group(required=False)
    behavior.add_argument(
        "--recreate",
        "-r",
        action="store_true",
        required=False,
        help="If the table already exists, recreate it as a way of updating it to the latest. "
        "Note: this will obviously remove the table in its current state.",
    )
    behavior.add_argument(
        "--migrate",
        "-m",
        type=int,
        required=False,
        help="If the table already exists, run a migration chain of spark scripts to update it. "
        "Must be negative values pulling from the end of the history "
        "(0 = all, -1 = last, -2 = 2nd from last, last). ",
    )
    return parser


def main(table, recreate=False, migrate=None):
    with SparkScriptSession() as spark:
        model = DELTA_MODELS[table](spark=spark)
        table_exists = model.exists()
        if migrate and not table_exists:
            raise ValueError("Migration provided but table doesn't exist.")
        elif migrate and migrate > 0:
            raise ValueError("Migration provided but not a negative value.")
        elif migrate and (-1 * migrate > len(model.migration_history)):
            raise ValueError("Migration exceeds the amount of table migrations available.")

        if table_exists and migrate:
            model.migrate(migrate)
        else:
            model.initialize(recreate=recreate)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create or migrate delta tables")
    parser = setup_parser(parser)
    args = parser.parse_args()

    main(args.table, args.recreate, args.migrate)
