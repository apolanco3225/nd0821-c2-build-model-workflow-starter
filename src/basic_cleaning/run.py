#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    data = pd.read_csv(artifact_local_path)

    # get indexes of rows with the allowed values
    idx = data['price'].between(args.min_price, args.max_price)
    # select rows based on the condition and make a copy
    data = data[idx].copy()

    # extra cleaning longitude column
    idx = data['longitude'].between(-74.25, -73.50) & data['latitude'].between(40.5, 41.2)
    # select rows based on the conditino and make a copy of the new dataframe
    data = data[idx].copy()

    # Convert last_review to datetime
    data['last_review'] = pd.to_datetime(data['last_review'])
    print(data.info())

    # save clean data in csv format
    data.to_csv("clean_sample.csv", index=False)

    # create a new artifact for clean data
    artifact = wandb.Artifact(
        name=args.output_artifact, 
        type=args.output_type, 
        description=args.output_description
    )

    # log artifact to W&B
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)


    # finish run
    run.finish()



    


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Data artifact that will be cleaned.",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Name of the output artifact after clearning.",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Type of the output data artifact after cleaning",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Description of the output artifact.",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimum value allowed for the price column.",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Maximum value allowed for the price column.",
        required=True
    )


    args = parser.parse_args()

    go(args)
