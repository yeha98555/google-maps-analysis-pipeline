# Taiwan Travel Attractions Analysis Data Pipeline

## Overview
This project develops a comprehensive data pipeline to analyze and visualize key metrics about travel attractions in Taiwan. Utilizing data processing technologies and cloud infrastructure, it incorporates data extracted from Google Maps reviews. This integration aims to provide valuable insights for tourists and businesses interested in the Taiwanese tourism sector.

## Prerequisites
Before running this project, you must have the following installed:

- Terraform (v1.8.5 or later)
- Docker (v26.1.4 or later)

## Installation
### Setup Terraform
Refer to the detailed instructions in [Terraform README](./terraform/README.md) for setting up Terraform.

### Setup Airflow
For setting up Airflow, follow the steps provided in [Airflow README](./airflow/README.md).

## Tech Stack
Technologies used in this project

- **Google Cloud**: Provides the computing and storage resources, specifically using Google Cloud Storage, BigQuery and Cloud Functions.
- **Terraform**: Manages the infrastructure as code.
- **Airflow**: Orchestrates and schedules the data pipeline workflows.
- **Python**: Used for scripting and data manipulation tasks, with key libraries including:
  - **Pandas**: For data manipulation and analysis.
  - **PyArrow**: For efficient data storage and retrieval.
  - **SQLAlchemy**: For database interaction.
  - **Psycopg2-binary**: For PostgreSQL database connectivity.
  - **jieba**: For Chinese text segmentation.
  - **SnowNLP**: For sentiment analysis of Chinese text.
