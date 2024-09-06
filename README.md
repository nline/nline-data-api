# nline-data-api

A lightweight utility to access the  GridWatch Accra Dataset collected by nLine Inc. over the course of July 2018 - July 2024.

## Description

nline-data-api provides easy access to GridWatch data collected in Ghana. This library offers functions to fetch, process, and analyze time-series data of voltage and frequency measurements across various sites, districts, and regions. Learn more about the data [here](https://nline.io/public-data).

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
uv init # initialize a project in your repository
git clone https://path-to-github-repo
cd nline-data-api # move to the cloned repo
uv sync
cd .. # move back to the root of the local repository
```

## Usage

First, create a Python script, for example, `main.py` from where to fetch, process, and analyze the GridWatch time-series data.

Within your script, import the necessary functions:

```py
from nline_data_api import fetch_data, time_series_average, spatial_group_summary, percentile_analysis, rolling_window_stats
```

### Fetching Data

To retrieve data for a specific time range:

```py
start_time = "2023-01-01 00:00"
end_time = "2023-01-07 00:00"
df = fetch_data(start_time, end_time)
```

**Note:** Due to the density of data, sensor data for a single day may range from 25-50mb compressed. For example, downloading a month of data might be around 1.25gb and at 25mb/s it would take roughly 1min 10s. You may want to filter by specific sites or districts.

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

## Sample Script to Fetch, Process, and Analyze Data
```py
import sys
import os

# Add nline-data-api to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), './nline-data-api/src/')))

# Import necessary functions
from nline_data_api import fetch_data, time_series_average, spatial_group_summary, percentile_analysis, rolling_window_stats # type: ignore

# Retrieve data for a specific time range
start_time = "2023-01-01 00:00"
end_time = "2023-01-07 00:00"
df = fetch_data(start_time, end_time)

# Calculate time series averages 
avg_df = time_series_average(df, group_by="district", time_interval="1h")
print(avg_df)

# Get spatial summaries
percentiles_df = percentile_analysis(df, group_by="site_id")
print(percentiles_df)

```
> **NOTE**

1. While running `main.py` above, you may be asked to install three missing modules: *polars*, *pyarrow*, and *requests*.
You can do so using `uv pip install [module]`.
  In the case of polars, install `polars-lts-cpu` should you get a warning that running *polars* will likely cause the program to crash. 

3. You may also need to create Google Cloud credentials if not set up.
   Confirm this by running `gcloud config configurations list`.
   If you do not have your credentials set up, first install the Google Cloud SDK (including the gcloud cli): 
    ```bash
    curl https://sdk.cloud.google.com | bash
    ```
   or visit the Google Cloud SDK [page](https://cloud.google.com/sdk/?hl=en).

4. Next, authenticate using
   ```bash
   gcloud auth application-default login
   ```
   
## API Key

On first use, you'll be prompted to enter your details to receive an API key. This key will be saved locally for future use.

You can optionally add the API key you received from [nline.io](https://nline.io/public-data) in a `.access_token` file in the root directory.

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

For any queries, please contact [info@nline.io](mailto:info@nline.io).
