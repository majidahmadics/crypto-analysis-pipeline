# Cryptocurrency Analysis Pipeline

An end-to-end cryptocurrency analysis system with ETL pipeline, machine learning models, and interactive dashboard.

## Features

- Extract historical cryptocurrency OHLCV data from yfinance API
- Collect cryptocurrency news from public APIs
- Transform and load data using Apache Airflow
- Build predictive models with MLflow and TFX
- Visualize data and predictions through an interactive dashboard
- Access data and predictions via RESTful API

## Project Structure

- `src/etl/`: ETL pipeline components
- `src/ml/`: Machine learning model components
- `src/api/`: API service components
- `src/dashboard/`: Interactive dashboard components
- `src/utils/`: Utility functions and helpers
- `config/`: Configuration files
- `data/`: Data storage
- `docs/`: Documentation
- `tests/`: Test suites
- `notebooks/`: Jupyter notebooks for exploration

## Setup and Installation

1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Configure environment variables
4. Run the pipeline: `python src/main.py`

## License

This project is licensed under [LICENSE] - see the LICENSE file for details.