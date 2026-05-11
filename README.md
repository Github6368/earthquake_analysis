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
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── dashboard/
│   ├── utility/
│   └── notebooks/
│       └── (rename of SDP_Earthquake_ana...)
│
└── README.md
```
## Directory Overview

- **architecture/**: Contains project architecture diagrams and visualization files
- **src/**: Main source code directory containing:
  - **ingestion/**: Data ingestion scripts
  - **bronze/**: Raw data layer
  - **silver/**: Cleaned and validated data layer
  - **gold/**: Aggregated and processed data layer
  - **dashboard/**: Dashboard and visualization code
  - **utility/**: Utility functions and helpers
  - **notebooks/**: Jupyter notebooks and analysis notebooks
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

```
# Let's create the HTTP connection using the SDK
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
## Import all the necessary libraries 
```
import requests
import json
import datetime
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog

w = WorkspaceClient()

# Let's define the API endpoint URL
url = f"{base_url}summary/all_day.geojson"
response = requests.get(url)

if response.status_code != 200:
    raise Exception(f"Error in getting data from {url}")

data = response.json()
current_date = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
print("current_date", current_date)
dbutils.fs.put(
    f"/Volumes/lakehouse/01_raw/raw/earthquake_data_{current_date}.json",
    json.dumps(data),
)
```


