from datetime import datetime, timedelta
from typing import List, Optional, Tuple

import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as fs

from .key_manager import APIKeyManager

GCS_BUCKET = "nline-public-data"
TOKEN_FILE = ".access_token"

key_manager = APIKeyManager()


def parse_datetime(date_string: str) -> datetime:
    """
    Parse a date string into a datetime object.

    Args:
        date_string: The date string to parse.

    Returns:
        A datetime object.

    Raises:
        ValueError: If the date string cannot be parsed.
    """
    formats = ["%Y-%m-%d %H:%M", "%Y-%m-%d"]
    for fmt in formats:
        try:
            return datetime.strptime(date_string, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unable to parse date string: {date_string}")


def get_csv_files(
    start_datetime: datetime, end_datetime: datetime
) -> List[Tuple[datetime.date, str]]:
    """
    Generate a list of CSV file paths for a given date range.

    Args:
        start_datetime: Start date and time as datetime object.
        end_datetime: End date and time as datetime object.

    Returns:
        A list of tuples, each containing a date and corresponding CSV file path.
    """
    start_date = start_datetime.date()
    end_date = end_datetime.date()
    date_range = [
        start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)
    ]
    return [
        (date, f"gs://{GCS_BUCKET}/ghana/gridwatch_data/csv/{date:%Y-%m-%d}.csv")
        for date in date_range
    ]


def process_csv(
    file_path: str,
    date: datetime.date,
    start_datetime: datetime,
    end_datetime: datetime,
) -> pl.LazyFrame:
    """
    Process a CSV file and filter data based on the given date range.

    Args:
        file_path: Path to the CSV file.
        date: The date of the CSV file.
        start_datetime: Start date and time as datetime object.
        end_datetime: End date and time as datetime object.

    Returns:
        A Polars LazyFrame with filtered data.
    """
    df = pl.scan_csv(file_path)

    if date == start_datetime.date():
        df = df.filter(pl.col("time") >= start_datetime.strftime("%Y-%m-%d %H:%M"))
    elif date == end_datetime.date():
        df = df.filter(pl.col("time") < end_datetime.strftime("%Y-%m-%d %H:%M"))

    return df


def fetch_data_by_csv(start_datetime: str, end_datetime: str) -> pl.DataFrame:
    """
    Retrieve and merge filtered data from multiple CSV files for a given date range.

    Args:
        start_datetime: Start date and time in format "%Y-%m-%d %H:%M".
        end_datetime: End date and time in format "%Y-%m-%d %H:%M".

    Returns:
        A Polars DataFrame with merged and filtered data.
    """
    if not key_manager.validate_or_retrieve_key():
        raise ValueError("Access token not found. Please register to use this API.")

    start = parse_datetime(start_datetime)
    end = parse_datetime(end_datetime)
    csv_files = get_csv_files(start, end)

    dfs = [process_csv(file, date, start, end) for date, file in csv_files]

    return pl.concat(dfs, how="vertical_relaxed").collect()


def fetch_data(
    start_datetime: str, end_datetime: str, filters: Optional[dict] = None
) -> pl.DataFrame:
    """Get data by using a PyArrow predicate filter on our partitioned dataset.

    Args:
        start_datetime: Start date and time in format "%Y-%m-%d %H:%M".
        end_datetime: End date and time in format "%Y-%m-%d %H:%M".
        filters: Dictionary of filters to apply. Format:
            {
                "column": value,  # For exact matches
                "column": {"op": "<|>|<=|>=|==|!=", "value": value}  # For comparisons
            }
            Example: {
                "district": "Mampong",  # Exact match
                "voltage": {"op": ">=", "value": 220},  # Comparison
                "site_id": [1, 2, 3]  # List for multiple values
            }

    Returns:
        Filtered Polars DataFrame
    """
    if not key_manager.validate_or_retrieve_key():
        raise ValueError("Access token not found. Please register to use this API.")

    # Convert to datetime
    start, end = map(parse_datetime, (start_datetime, end_datetime))

    source = f"{GCS_BUCKET}/ghana/gridwatch_data/parquet"

    partitioning = ds.partitioning(
        pa.schema([("day", pa.timestamp("s"))]), flavor="hive"
    )
    gcs_fs = fs.GcsFileSystem(anonymous=True)
    dataset = ds.dataset(source, filesystem=gcs_fs, partitioning=partitioning)

    start_day, end_day = (
        pa.scalar(d.replace(hour=0, minute=0, second=0)) for d in (start, end)
    )

    # Case to proper format - speeds up filtering a bit
    filtered_dataset = dataset.filter(
        (ds.field("day") >= start_day) & (ds.field("day") < end_day)
    )

    # Start with time-based filter
    query = (pl.col("time") >= start) & (pl.col("time") < end)

    # Add user-specified filters
    if filters:
        for column, condition in filters.items():
            if isinstance(condition, dict):
                # Handle comparison operators
                op = condition["op"]
                value = condition["value"]
                if op == ">":
                    query &= pl.col(column) > value
                elif op == ">=":
                    query &= pl.col(column) >= value
                elif op == "<":
                    query &= pl.col(column) < value
                elif op == "<=":
                    query &= pl.col(column) <= value
                elif op == "==":
                    query &= pl.col(column) == value
                elif op == "!=":
                    query &= pl.col(column) != value
                else:
                    raise ValueError(f"Unsupported operator: {op}")
            elif isinstance(condition, (list, tuple)):
                # Handle list of values (IN clause)
                query &= pl.col(column).is_in(condition)
            else:
                # Handle exact match
                query &= pl.col(column) == condition

    result = pl.scan_pyarrow_dataset(filtered_dataset).filter(query).collect()

    return result


def time_series_average(
    df: pl.DataFrame,
    group_by: Optional[str] = None,
    time_interval: str = "1h",
    metrics: List[str] = ["voltage", "frequency"],
) -> pl.DataFrame:
    """
    Calculate average metrics over time for a given spatial grouping or the entire dataset.

    Args:
        df: Polars DataFrame.
        group_by: Column to group by ('district', 'region', or 'site_id'). If None, averages entire dataset.
        time_interval: Time interval for grouping (default: '1h').
        metrics: Metrics to average (default: ['voltage', 'frequency']).

    Returns:
        A Polars DataFrame with time series averages.
    """
    time_col = pl.col("time").cast(pl.Datetime).dt.truncate(time_interval)
    agg_exprs = [
        pl.col(metric).drop_nulls().mean().alias(f"avg_{metric}") for metric in metrics
    ]

    if group_by:
        return df.group_by([group_by, time_col]).agg(agg_exprs)
    else:
        return df.group_by(time_col).agg(agg_exprs)


def spatial_group_summary(
    df: pl.DataFrame,
    group_by: Optional[str] = None,
    metrics: List[str] = ["voltage", "frequency"],
) -> pl.DataFrame:
    """
    Calculate summary statistics for a given spatial grouping or the entire dataset.

    Args:
        df: Polars DataFrame.
        group_by: Column to group by ('district', 'region', or 'site_id'). If None, summarizes entire dataset.
        metrics: Metrics to summarize (default: ['voltage', 'frequency']).

    Returns:
        A Polars DataFrame with summary statistics.
    """
    agg_exprs = []
    for metric in metrics:
        agg_exprs.extend(
            [
                pl.col(metric).mean(ignore_nulls=True).alias(f"avg_{metric}"),
                pl.col(metric).min(ignore_nulls=True).alias(f"min_{metric}"),
                pl.col(metric).max(ignore_nulls=True).alias(f"max_{metric}"),
                pl.col(metric).std(ignore_nulls=True).alias(f"std_{metric}"),
            ]
        )

    if group_by:
        return df.group_by(group_by).agg(agg_exprs)
    else:
        return df.select(agg_exprs)


def percentile_analysis(
    df: pl.DataFrame,
    group_by: Optional[str] = None,
    metrics: List[str] = ["voltage", "frequency"],
    percentiles: List[float] = [0.25, 0.5, 0.75],
) -> pl.DataFrame:
    """
    Calculate percentiles for given metrics, optionally grouped by a column.

    Args:
        df: Polars DataFrame.
        group_by: Column to group by. If None, analyzes entire dataset.
        metrics: Metrics to analyze (default: ['voltage', 'frequency']).
        percentiles: List of percentiles to calculate (default: [0.25, 0.5, 0.75]).

    Returns:
        A Polars DataFrame with percentile statistics.
    """
    agg_exprs = [
        pl.col(metric).quantile(p).alias(f"{metric}_p{int(p*100)}")
        for metric in metrics
        for p in percentiles
    ]

    if group_by:
        return df.group_by(group_by).agg(agg_exprs)
    else:
        return df.select(agg_exprs)


def rolling_window_stats(
    df: pl.DataFrame,
    window_size: str = "24h",
    metrics: List[str] = ["voltage", "frequency"],
) -> pl.DataFrame:
    """
    Calculate rolling window statistics for the entire dataset.

    Args:
        df: Polars DataFrame.
        window_size: Size of the rolling window (default: "24h").
        metrics: Metrics to analyze (default: ['voltage', 'frequency']).

    Returns:
        A Polars DataFrame with rolling window statistics.
    """
    return df.sort("time").select(
        "time",
        *[
            pl.col(metric).rolling_mean(window_size).alias(f"{metric}_rolling_mean")
            for metric in metrics
        ],
        *[
            pl.col(metric).rolling_std(window_size).alias(f"{metric}_rolling_std")
            for metric in metrics
        ],
    )
