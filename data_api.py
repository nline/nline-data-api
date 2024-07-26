from datetime import datetime, timedelta

import dask.dataframe as dd
import pandas as pd
import polars as pl
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from google.cloud import storage
from pyarrow.fs import GcsFileSystem
from pyarrow.parquet import ParquetDataset

gcs_nline = "gs://nline-public-data"


def parse_datetime(date_string):
    formats = ["%Y-%m-%d", "%Y-%m-%d %H:%M"]
    for fmt in formats:
        try:
            return datetime.strptime(date_string, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unable to parse date string: {date_string}")


def get_raw_polars(bucket_name, start_time, end_time):
    source = f"{gcs_nline}/{bucket_name}/*.parquet"

    # Convert start_time and end_time to datetime objects
    start = parse_datetime(start_time)
    end = parse_datetime(end_time)

    # Use Polars to read and filter the Parquet files
    df = pl.scan_parquet(source)
    filtered_df = df.filter((pl.col("time") >= start) & (pl.col("time") < end))

    # Return the lazy DataFrame
    return filtered_df


def get_raw_dask(bucket_name, start_time, end_time):
    source = f"{gcs_nline}/{bucket_name}/*.parquet"

    # Convert start_time and end_time to datetime objects
    start = parse_datetime(start_time)
    end = parse_datetime(end_time)

    # Read the Parquet files into a Dask DataFrame
    time_filter = [("time", ">=", start), ("time", "<", end)]

    df = dd.read_parquet(source, filters=time_filter)
    # df = dd.read_parquet(source)
    # df = df[df['time'].between(start, end)]
    # Compute the result (this will trigger the actual read and filter operations)
    # result = df.compute()

    return df


def get_raw(bucket_name, start_time, end_time):
    source = f"{gcs_nline}/{bucket_name}"
    # source = f"{gcs_nline}/{bucket_name}/"
    # source = source.replace("gs://", "")

    # gcs = GcsFileSystem()
    # dataset = ds.dataset(source, format="parquet", filesystem=gcs)

    # Convert start_time and end_time to datetime objects
    start = parse_datetime(start_time)
    end = parse_datetime(end_time)

    dataset = ds.dataset(source, format="parquet")
    filtered_dataset = dataset.filter(
        (ds.field("time") >= start) & (ds.field("time") < end)
    )

    df = pl.scan_pyarrow_dataset(filtered_dataset)

    return df


def get_csv_files(start_datetime, end_datetime):
    start = datetime.strptime(start_datetime, "%Y-%m-%d %H:%M")
    end = datetime.strptime(end_datetime, "%Y-%m-%d %H:%M")
    date_range = [
        start.date() + timedelta(days=x)
        for x in range((end.date() - start.date()).days + 1)
    ]
    return [
        (
            date,
            f"gs://nline-public-data/ghana/gridwatch_data/{date.strftime('%Y-%m-%d')}.csv",
        )
        for date in date_range
    ]


def process_csv(file_path, date, start_datetime, end_datetime):
    df = pl.read_csv(file_path)

    if date == datetime.strptime(start_datetime, "%Y-%m-%d %H:%M").date():
        df = df.filter(pl.col("time") >= start_datetime)
    elif date == datetime.strptime(end_datetime, "%Y-%m-%d %H:%M").date():
        df = df.filter(pl.col("time") < end_datetime)

    return df


def get_filtered_data(start_datetime, end_datetime):
    csv_files = get_csv_files(start_datetime, end_datetime)

    dataframes = []
    for date, file in csv_files:
        print("Loading", file)
        df = process_csv(file, date, start_datetime, end_datetime)
        dataframes.append(df)

    return pl.concat(dataframes)


def average_by_spatial_group(df, group_by, metrics=["voltage", "frequency"]):
    """
    Calculate average metrics for a given spatial grouping.

    :param df: Polars DataFrame
    :param group_by: str, column to group by ('district', 'region', or 'site_id')
    :param metrics: list of str, metrics to average (default: ['voltage', 'frequency'])
    :return: Polars DataFrame with averages
    """
    return df.groupby(group_by).agg(
        [pl.col(metric).mean().alias(f"avg_{metric}") for metric in metrics]
    )


def time_series_average(
    df, group_by, time_interval="1h", metrics=["voltage", "frequency"]
):
    """
    Calculate average metrics over time for a given spatial grouping.

    :param df: Polars DataFrame
    :param group_by: str, column to group by ('district', 'region', or 'site_id')
    :param time_interval: str, time interval for grouping (default: '1h')
    :param metrics: list of str, metrics to average (default: ['voltage', 'frequency'])
    :return: Polars DataFrame with time series averages
    """
    return df.groupby([group_by, pl.col("time").dt.truncate(time_interval)]).agg(
        [pl.col(metric).mean().alias(f"avg_{metric}") for metric in metrics]
    )


def spatial_group_summary(df, group_by, metrics=["voltage", "frequency"]):
    """
    Calculate summary statistics for a given spatial grouping.

    :param df: Polars DataFrame
    :param group_by: str, column to group by ('district', 'region', or 'site_id')
    :param metrics: list of str, metrics to summarize (default: ['voltage', 'frequency'])
    :return: Polars DataFrame with summary statistics
    """
    return df.groupby(group_by).agg(
        [
            pl.col(metric).mean().alias(f"avg_{metric}"),
            pl.col(metric).min().alias(f"min_{metric}"),
            pl.col(metric).max().alias(f"max_{metric}"),
            pl.col(metric).std().alias(f"std_{metric}"),
        ]
        for metric in metrics
    )
