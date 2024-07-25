from datetime import datetime

import pandas as pd
import polars as pl
import pyarrow.dataset as ds
import pyarrow.parquet as pq
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


def get_raw_lazy(bucket_name, start_time, end_time):
    source = f"{gcs_nline}/{bucket_name}/*.parquet"

    # Convert start_time and end_time to datetime objects
    start = parse_datetime(start_time)
    end = parse_datetime(end_time)

    # Scan the Parquet files lazily
    df = pl.scan_parquet(source)

    # Apply time filter
    df_filtered = df.filter((pl.col("time") >= start) & (pl.col("time") < end))

    # Collect the filtered data
    result = df_filtered.collect()

    return result


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
    filtered_dataset = dataset.filter((ds.field("time") >= start) & (ds.field("time") < end))
    
    df = pl.scan_pyarrow_dataset(filtered_dataset)

    return df


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
