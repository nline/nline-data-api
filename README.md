# nline-data-api

A lightweight utility to access the MCC public data collected by nLine Inc.

## Description

nline-data-api provides easy access to gridwatch data collected in Ghana. This library offers functions to fetch, process, and analyze time-series data related to voltage and frequency measurements across various sites, districts, and regions.

## Installation

We use `uv` for project management. To install `uv`:

```sh
# On macOS and Linux.
$ curl -LsSf https://astral.sh/uv/install.sh | sh

# On Windows.
$ powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# With pip.
$ pip install uv
```

To then install the project:

```sh
uv venv
uv sync
```

## Usage

First, import the necessary functions:

```py
from nline_data_api import fetch_data, time_series_average, spatial_group_summary, percentile_analysis, rolling_window_stats
```

### Fetching Data

To retrieve data for a specific time range:

```py
start_time = "2023-01-01 00:00"
end_time = "2023-01-02 00:00"
df = fetch_data(start_time, end_time)
```

### Data Analysis

1. Calculate time series averages:

```py
avg_df = time_series_average(df, group_by="district", time_interval="1h")
```

2. Get spatial group summaries:

```py
summary_df = spatial_group_summary(df, group_by="region")
```

3. Perform percentile analysis:

```py
percentiles_df = percentile_analysis(df, group_by="site_id")
```

4. Calculate rolling window statistics:

```py
rolling_stats_df = rolling_window_stats(df, window_size="24h")
```

## API Key

On first use, you'll be prompted to enter your details to receive an API key. This key will be saved locally for future use.

## Data Description

The dataset includes the following main columns:

- time: Timestamp of the measurement
- voltage: Voltage measurement
- frequency: Frequency measurement
- site_id: Unique identifier for each measurement site
- district: District where the site is located
- region: Region where the site is located

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Contact

For any queries, please contact info@nline.io.
