# Earthquake Analysis
This is a repo for databricks project on earthquake analysis

## Project Structure Overview

```
earthquake_analysis/
│
├── architecture/
│   ├── architecture.excalidraw
│   ├── earthquake_analysis_architecture.png
│   └── earthquake_analysis_project_flow.png
│
├── src/
│   ├── Raw_ingestion/
│   ├── Bronze_consumtption/
│   ├── Silver_layer/
│   ├── Gold_layer/
│   ├── Dashboard/
│   ├── Utility/
│   └── notebooks/
│       └── (rename of SDP_Earthquake_ana...)
│
└── README.md
```
## Directory Overview

- **architecture/**: Contains project architecture diagrams and visualization files
- **src/**: Main source code directory containing:
  - **Raw_ingestion/**: Data ingestion scripts
  - **Bronze_consumption/**: Raw data layer
  - **Silver_layer/**: Cleaned and validated data layer
  - **Gold_layer/**: Aggregated and processed data layer
  - **Dashboard/**: Dashboard and visualization code
  - **Utility/**: Utility functions and helpers
  - **Notebooks/**: Jupyter notebooks and analysis notebooks

## Step - I
## Here is our first notebook as Raw_ingestion
## Now, let's put all the necessary elements into a variable for betterment
```
# Now, let's create a connection with the use of code snippets
source_details = {
    "host": "https://earthquake.usgs.gov",
    "port": 443, 
    "base_path": '/earthquakes/feed/v1.0/',  
    "bearer_token": 'na'
}

```
## Step - II
# Let's create the HTTP connection using the SDK
```
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog

w = WorkspaceClient()
w.connections.create(
    name="earthquake_api_http_connection_new",
    connection_type=catalog.ConnectionType.HTTP,
    options={
        "host": source_details["host"],
        "port": source_details["port"],
        "base_path": source_details["base_path"],
        "bearer_token": source_details.get("bearer_token", ""),
    }   
)
```
## Step - III
## Import all the necessary libraries 
```
# Let's import the necessary libraries to create the connection
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog

w = WorkspaceClient()
# We need to establish a connection to the API first to create the base api
conn = w.connections.get("earthquake_api_http_connection_new")
# earthquake_api_http_connection_new
base_url = f"{conn.options['host']}{conn.options['base_path']}"
```
## Step - IV
```
# Step - IV
# dbutils.widgets.text("api_source", "/summary/all_day.geojson")


api_source = dbutils.widgets.get("api_source")
# api_source = #/summary/all_day.geojson
print(api_source)
```
## Step - V
```
# Step - V
# These are the following considerating the best practices while ingesting the data
# 1. Add retry logic for transient API failures.
# 2. Parameterize the output path for flexibility.
# 3. Validate the JSON schema before saving.
# 4. Log metadata (e.g., record count, fetch time) for auditing.
# 5. Use Delta Lake for versioned, queryable storage.
# 6. Schedule this notebook as a Databricks job for automation.
# 7. Add error handling for file write failures.
# 8. Mask or redact sensitive data if present.
# 9. Implement incremental ingestion if supported by the API.
# 10. Monitor API rate limits and handle accordingly.

import requests
import json
import datetime

url = f"{base_url}{api_source.lstrip('/')}"

response = requests.get(url)

if response.status_code != 200:
    raise Exception(f"Error in getting data from {url}")

data = response.json()

current_date = datetime.datetime.now().strftime("%Y-%m-%d-%HH-%MM-%SS")
print("current_date", current_date)
dbutils.fs.put(
    f"/Volumes/lakehouse/01_raw/raw/earthquake_data_{current_date}.json",
    json.dumps(data),
)
current_date = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
print("current_date", current_date)
dbutils.fs.put(
    f"/Volumes/lakehouse/01_raw/raw/earthquake_data_{current_date}.json",
    json.dumps(data),
)

```
## Now, let' create another notebook to have our bronze layer ready
## Bronze_consumption Notebook
```
# Let's create directory using the dbutils.fs.mkdirs() method
# Checkpoint directory for structured streaming
# To create a directory

dbutils.fs.mkdirs("/Volumes/lakehouse/01_raw/raw/_checkpoint")
```

