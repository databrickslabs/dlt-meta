"""Main entry point of the Python Wheel."""
import logging
import argparse
from src.onboard_dataflowspec import OnboardDataflowspec
from pyspark.sql import SparkSession

logger = logging.getLogger("dlt-meta")
logger.setLevel(logging.INFO)

arguments = ["--onboard_layer",
             "--onboarding_file_path",
             "--database",
             "--env",
             "--bronze_dataflowspec_table",
             "--bronze_dataflowspec_path",
             "--silver_dataflowspec_table",
             "--silver_dataflowspec_path",
             "--import_author",
             "--version",
             "--overwrite",
             "--uc_enabled",
             ]


def parse_args():
    """Parse command line."""
    parser = argparse.ArgumentParser()
    for argument in arguments:
        parser.add_argument(argument)
    args = parser.parse_args()
    logger.info(f"Input arguments dict: {args}")
    return args


def main():
    """Whl file entry point."""
    args = parse_args()
    onboard_layer = args.__getattribute__("onboard_layer")
    uc_enabled = True if args.__getattribute__("uc_enabled").lower() == "true" else False
    onboarding_args_dict = args.__dict__
    del onboarding_args_dict['onboard_layer']
    del onboarding_args_dict['uc_enabled']
    if uc_enabled:
        del onboarding_args_dict['bronze_dataflowspec_path']
        del onboarding_args_dict['silver_dataflowspec_path']
    spark = (SparkSession.builder.appName("DLT-META_Onboarding_Task")).getOrCreate()
    onboard_obj = OnboardDataflowspec(spark, onboarding_args_dict, uc_enabled=uc_enabled)

    if onboard_layer.lower() == "bronze_silver":
        onboard_obj.onboard_dataflow_specs()
    elif onboard_layer.lower() == "bronze":
        onboard_obj.onboard_bronze_dataflow_spec()
    elif onboard_layer.lower() == "silver":
        onboard_obj.onboard_silver_dataflow_spec()
    else:
        raise Exception("onboard_layer argument missing in commandline")


if __name__ == "__main__":
    main()
