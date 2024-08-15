# FastAPI Application for League Data and Analytics

This FastAPI application provides various endpoints to manage and analyze league data. It handles tasks such as extracting player data, computing metrics and odds, and saving data to JSONL and CSV files.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [API Endpoints](#api-endpoints)
- [Logging](#logging)
- [License](#license)

## Installation

1. **Clone the repository:**

    ```bash
    git clone <repository-url>
    cd <repository-directory>
    ```

2. **Create and activate a virtual environment:**

    ```bash
    python3 -m venv env
    source env/bin/activate
    ```

3. **Install the dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

## Usage

1. **Start the FastAPI server:**

    ```bash
    uvicorn app:app --reload
    ```

    By default, the server will run on `http://127.0.0.1:8000`.

2. **Access the interactive API documentation:**

    Once the server is running, go to `http://127.0.0.1:8000/docs` to explore the API using the automatically generated Swagger UI.

## API Endpoints

### `/suze/analytics/odds`
- **Method:** GET
- **Description:** Computes the odds of winning a classic league and saves the results to a CSV file.
- **Output:** Saves the file `player_odds.csv` in the `data/` directory.

### `/suze/analytics/features`
- **Method:** GET
- **Description:** Extracts features from players' history and saves them for further analysis.
- **Output:** Saves the file `player_histories_and_metrics.csv` in the `data/` directory.

### `/suze/classic-league/players`
- **Method:** GET
- **Description:** Extracts and writes player data to a JSONL file.
- **Output:** Saves the file `players.jsonl` in the `data/` directory.

### `/suze/classic-league/player_history`
- **Method:** GET
- **Description:** Extracts and writes player history data to a JSONL file.
- **Output:** Saves the file `player_history.jsonl` in the `data/` directory.

### `/suze/classic-league/transfer_history`
- **Method:** GET
- **Description:** Extracts and writes player transfer history data to a JSONL file.
- **Output:** Saves the file `transfer_history.jsonl` in the `data/` directory.

### `/suze/classic-league/picks_history/{gw_number}`
- **Method:** GET
- **Description:** Extracts and writes picks history data for a given game week to a JSONL file.
- **Path Parameters:** 
  - `gw_number` (str): Game week number for which to extract picks history.
- **Output:** Saves the file `picks_history.jsonl` in the `data/` directory.

### `/suze/classic-league/{league_id}`
- **Method:** GET
- **Description:** Fetches and writes classic league data to a JSONL file.
- **Path Parameters:** 
  - `league_id` (str): The ID of the league to fetch data for.
- **Output:** Saves the file `classic_league.jsonl` in the `data/` directory.

### `/suze/h2h-league/{league_id}`
- **Method:** GET
- **Description:** Fetches and writes head-to-head league data to a JSONL file.
- **Path Parameters:** 
  - `league_id` (str): The ID of the head-to-head league to fetch data for.
- **Output:** Saves the file `h2h_league_[league_id].jsonl` in the `data/` directory.

## Logging

Logging is configured to capture application logs and save them to `app.log` in the root directory. The log level is set to `INFO`, but `DEBUG` logs are also captured for certain operations.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
