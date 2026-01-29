Spotify End-to-End Azure Data Engineering Project

📌 Project Overview

This project demonstrates an end-to-end Azure Data Engineering pipeline built using a Spotify-style dataset. The goal was to understand real-world cloud data engineering workflows — from raw data ingestion to analytics-ready datasets — using production-oriented tools and best practices.

The pipeline follows a Lakehouse (Bronze–Silver–Gold) architecture and is designed to be scalable, modular, and suitable for BI and analytics use cases.

🏗 Architecture & Workflow

1️⃣ Source & Ingestion

Source data stored in Azure SQL Database

Used Azure Data Factory (ADF) to ingest data into ADLS Gen2

Implemented incremental / CDC-based ingestion

Used ForEach loops and alerts to handle multiple tables and monitor pipeline execution

2️⃣ Data Lake Architecture

Implemented Medallion Architecture:

Bronze – raw ingested data

Silver – cleaned and transformed data

Gold – analytics-ready datasets

Standardized storage formats using Parquet and Delta Lake

3️⃣ Data Transformations with Databricks

Built ETL pipelines using PySpark

Designed modular and reusable transformation code

Used Databricks Auto Loader for scalable file ingestion

4️⃣ Delta Live Tables (DLT)

Created automated DLT pipelines for:

dimuser

dimtrack

dimdate

factstream

Added data quality expectations

Implemented SCD Type 2 / Auto-CDC to track historical changes

5️⃣ Data Modeling for Analytics

Designed a star schema in the Gold layer

Created fact and dimension tables optimized for analytics

Prepared data for BI tools and downstream consumption

6️⃣ CI/CD & Deployment

Used Databricks CLI (DAB Bundles) for automated deployment

Managed code using GitHub

Followed a clean, production-style repository structure

🧰 Tech Stack

Azure SQL Database

Azure Data Factory

Azure Data Lake Storage Gen2

Azure Databricks

PySpark

Delta Lake & Delta Live Tables

GitHub

Databricks CLI (DAB)

🎯 Key Learnings

Designing scalable ingestion pipelines using ADF

Implementing Lakehouse architecture on Azure

Building production-style ETL pipelines in Databricks

Managing data quality and historical tracking with DLT

Structuring projects for CI/CD and maintainability

