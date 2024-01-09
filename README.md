# ETL Project with Apache Airflow

## Overview

This repository contains the source code and configurations for an Extract, Transform, Load (ETL) project implemented using Apache Airflow. The project focuses on extracting data from various file formats (CSV, TSV, fixed-width), applying basic transformations, and loading the processed data into another CSV file.

## Requirements

Before getting started, make sure you have the following prerequisites installed:

- [WSL 2](https://docs.microsoft.com/en-us/windows/wsl/install)
- [Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/installation.html)

## Project Structure

/etl_project
├── dags
│ ├── main_etl_dag.py # Main Airflow DAG file
│ └── ...
├── config
│ ├── airflow.cfg # Airflow configuration file
│ └── ...
├── requirements.txt # Python dependencies
└── README.md # Project documentation



## Getting Started

1. Clone the repository:

   ```bash
   git clone https://github.com/your-username/etl_project.git
   
2. Install Python dependencies:

   pip install -r requirements.txt

3. Configure Airflow:

    Update the airflow.cfg file in the config folder with your specific configurations.

4. Start Airflow:

    airflow standalone

Access the Airflow web UI at http://localhost:8080 to monitor and trigger the DAG.



Feel free to modify the template according to your project's specific details and requirements. Add more sections as needed, such as data source details, transformation logic, or any other relevant information.



