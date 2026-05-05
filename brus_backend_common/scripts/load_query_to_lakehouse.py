import argparse
import logging

from brus_backend_common.models import LAKEHOUSE_MODELS
from brus_backend_common.helpers.spark import SparkScriptSession

logger = logging.getLogger(__name__)


def setup_parser(parser):
    """Separating parser functionality as USAS uses Django Commands"""
    parser.add_argument(
        "--table",
        "-t",
        type=str,
        required=True,
        help="The destination LakeHouse Table to write the data",
        choices=list(LAKEHOUSE_MODELS.keys()),
    )
    parser.add_argument(
        "--incremental",
        "-i",
        required=False,
        action="store_true",
        help="Whether to incrementally add to the table or repopulate entirely",
    )
    return parser


def main(table, incremental=False):
    with SparkScriptSession() as spark:
        model = LAKEHOUSE_MODELS[table](spark=spark)
        table_exists = model.exists()
        if not table_exists:
            raise ValueError("Table doesn't exist. Use create_migrate_lakehouse_table beforehand.")

        if incremental:
            model.increment()
        else:
            model.repopulate()


if __name__ == "__main__":
    configure_logging()
    parser = argparse.ArgumentParser(description="Populate a lakehouse table with its designated query.")
    parser = setup_parser(parser)
    args = parser.parse_args()

    main(args.table, args.incremental)
