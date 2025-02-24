# ELT Case Study Pipeline

This repository contains an ELT (Extract, Load, Transform) pipeline designed to process sales data. The pipeline ingests raw data, applies necessary transformations, and prepares it for analysis.

## Project Structure

The repository is organised as follows:

```bash
├── .dlt/                     # Configuration and metadata for the data loading tool (dlt)
├── data/
│   ├── raw/                  # Directory containing raw input data files
├── logs/                      # Directory for storing log files generated during pipeline execution
├── transformation/            # Contains transformation scripts to process raw data
├── .gitignore                 # Specifies files and directories to be ignored by Git
├── ingestion_pipeline.py      # Script responsible for extracting and loading data into the system
├── requirement.txt            # Lists the Python dependencies required for the project
├── transformation_pipeline.py # Script that defines the data transformation processes
└── README.md 
```

## Getting Started

To set up and run the ELT pipeline on your local machine, follow these steps:

### Prerequisites

- Python 3.11 or higher
- pip (Python package installer)

### Installation

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/pipelinetoinsights/elt-pipeline-with-dlt-and-dbt.git
   cd elt-case-study-pipeline
   ```

2. **Install Dependencies:**

   ```bash
   pip install -r requirement.txt
   ```
3. **create secrets.toml file in .dlt and fill out the postgres info**

```bash
[destination.postgres.credentials]
database = "" # please set me up!
password = "" # please set me up!
username = "" # please set me up!
host = "" # please set me up!
port = 
connect_timeout = 15

   ```
### Running the Pipeline

1. **Data Ingestion:**

Execute the ingestion script to extract raw data and load it into the system:

```bash
   python ingestion_pipeline.py
```
   This script reads data from the `data/raw/` directory and loads it into the appropriate storage.

2. **Data Transformation:**

   Run the transformation script to process and clean the ingested data:

   ```bash
   python transformation_pipeline.py
   ```
   This will apply necessary transformations and output the processed data ready for analysis.
