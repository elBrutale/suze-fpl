from fastapi import FastAPI, HTTPException
from data_io.league import get_classic_league_data, get_h2h_league_data
from data_io.players import extract_player_data, get_player_history, get_transfer_history, get_picks_history

from analytics.utils import jsonl_to_df, read_dataframe
from analytics.odds_classic import calculate_metrics, calculate_odds

import os
import json
import logging

# Initialize the FastAPI app
app = FastAPI()

# Configure logging
logging.basicConfig(
    filename='app.log',  # Log file name
    level=logging.INFO,  # Log level
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Log format
)

# Get a logger instance
logger = logging.getLogger(__name__)

@app.get("/suze/analytics/odds")
async def compute_odds():
    try:
        logger.info("Received request to compute odds of winning classic")
        # Define the input CSV file paths
        input_file = os.path.join("data", "player_histories_and_metrics.csv")
        output_file = os.path.join("data", "player_odds.csv")
        # Load the CSV file into a DataFrame
        df = read_dataframe(input_file)
        # Compute the odds of winning the classic league
        df_odds = calculate_odds(df)
        df_odds.to_csv(output_file, index=False, encoding='utf-8')
    except Exception as e:
        logger.error(f"Failed to compute odds of winning classic. Error: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {e}")

@app.get("/suze/analytics/features")
async def compute_features():
    try:
        logger.info("Received request to extract features from players history")

        # Ensure the data directory exists
        os.makedirs("data", exist_ok=True)

        # Define file paths
        parsed_players_file = os.path.join("data", "players.jsonl")
        player_histories_file = os.path.join("data", "player_history.jsonl")

        # Read the JSONL files into dataframes
        parsed_players_df = jsonl_to_df(parsed_players_file)
        player_histories_df = jsonl_to_df(player_histories_file)

        # Compute features for odds computation
        features_df = calculate_metrics(player_histories_df, parsed_players_df)

        # Define the output CSV file path
        output_csv_file = os.path.join("data", "player_histories_and_metrics.csv")

        # Write the resulting DataFrame to a CSV file
        features_df.to_csv(output_csv_file, index=False)

    except Exception as e:
        logger.error(f"Failed to compute features for odds computation. Error: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {e}")

@app.get("/suze/classic-league/players")
async def write_players_file():
    try:
        logger.info("Received request to extract and write player data")

        # Ensure the data directory exists
        os.makedirs("data", exist_ok=True)

        # Define file paths
        input_file_path = os.path.join("data", "classic_league.jsonl")
        output_file_path = os.path.join("data", "players.jsonl")

        # Extract player data and write to the output file
        with open(input_file_path, 'r') as infile, open(output_file_path, 'a') as outfile:
            for line in infile:
                json_data = json.loads(line)
                players = extract_player_data(json_data)
                for player in players:
                    outfile.write(json.dumps(player) + '\n')
                    logger.debug(f"Written player data to file: {player}")

        logger.info(f"Successfully wrote player data to {output_file_path}")
        return {"message": "Players data written successfully"}

    except Exception as e:
        logger.error(f"Failed to write player data. Error: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {e}")


@app.get("/suze/classic-league/player_history")
async def write_player_history_file():
    try:
        logger.info("Received request to extract and write player history data")

        # Define file paths
        input_file_path = os.path.join("data", "players.jsonl")
        output_file_path = os.path.join("data", "player_history.jsonl")

        # Extract player history and write to the output file
        with open(input_file_path, 'r') as infile, open(output_file_path, 'a') as outfile:
            for line in infile:
                player = json.loads(line)
                player_history = get_player_history(player['entry_id'])
                outfile.write(json.dumps(player_history) + '\n')
                logger.debug(f"Written player history data for entry_id: {player['entry_id']}")

        logger.info(f"Successfully wrote player history data to {output_file_path}")
        return {"message": "Player history data written successfully"}

    except Exception as e:
        logger.error(f"Failed to write player history data. Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    

@app.get("/suze/classic-league/transfer_history")
async def write_transfer_history_file():
    try:
        logger.info("Received request to extract and write transfer history data")

        # Define file paths
        input_file_path = os.path.join("data", "players.jsonl")
        output_file_path = os.path.join("data", "transfer_history.jsonl")

        # Extract transfer history and write to the output file
        with open(input_file_path, 'r') as infile, open(output_file_path, 'a') as outfile:
            for line in infile:
                player = json.loads(line)
                player_history = get_transfer_history(player['entry_id'])
                outfile.write(json.dumps(player_history) + '\n')
                logger.debug(f"Written transfer history data for entry_id: {player['entry_id']}")

        logger.info(f"Successfully wrote transfer history data to {output_file_path}")
        return {"message": "Transfer history data written successfully"}

    except Exception as e:
        logger.error(f"Failed to write transfer history data. Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    

@app.get("/suze/classic-league/picks_history/{gw_number}")
async def write_picks_history_file(gw_number: str):
    try:
        logger.info("Received request to extract and write picks history data")

        # Define file paths
        input_file_path = os.path.join("data", "players.jsonl")
        output_file_path = os.path.join("data", "picks_history.jsonl")

        # Extract picks history and write to the output file
        with open(input_file_path, 'r') as infile, open(output_file_path, 'a') as outfile:
            for line in infile:
                player = json.loads(line)
                picks_history = get_picks_history(gw_number=gw_number, entry_id=player['entry_id'])
                outfile.write(json.dumps(picks_history) + '\n')
                logger.debug(f"Written picks history data for entry_id: {player['entry_id']}")

        logger.info(f"Successfully wrote picks history data to {output_file_path}")
        return {"message": "Picks history data written successfully"}

    except Exception as e:
        logger.error(f"Failed to write picks history data. Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
            

@app.get("/suze/classic-league/{league_id}")
async def write_league_file(league_id: str):
    try:
        logger.info(f"Received request to fetch and write classic league data for league_id: {league_id}")
        
        # Call your function from get_classic_league to get the data
        league_data = get_classic_league_data(league_id=league_id)
        logger.info(f"Successfully retrieved data for league_id: {league_id}")

        # Ensure the data directory exists
        os.makedirs("data", exist_ok=True)

        # Write to the file
        # Append each JSON object to the JSONL file
        file_path = os.path.join("data", "classic_league.jsonl")
        with open(file_path, 'a') as f:
            for data in league_data:
                f.write(json.dumps(data) + '\n')
                logger.debug(f"Written data to file for league_id: {league_id} - Data: {data}")

        logger.info(f"Successfully wrote data for league_id: {league_id} to {file_path}")
        return {"message": "Classic league written successfully"}

    except Exception as e:
        logger.error(f"Failed to write data for league_id: {league_id}. Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    
@app.get("/suze/h2h-league/{league_id}")
async def write_h2h_file(league_id: str):
    try:
        logger.info(f"Received request to fetch and write h2h league data for league_id: {league_id}")
        
        # Call your function from get_classic_league to get the data
        league_data = get_h2h_league_data(league_id=league_id)
        logger.info(f"Successfully retrieved data for league_id: {league_id}")

        # Ensure the data directory exists
        os.makedirs("data", exist_ok=True)

        # Write to the file
        # Append each JSON object to the JSONL file
        file_path = os.path.join("data", f"h2h_league_[league_id].jsonl")
        with open(file_path, 'a') as f:
            for data in league_data:
                f.write(json.dumps(data) + '\n')
                logger.debug(f"Written data to file for league_id: {league_id} - Data: {data}")

        logger.info(f"Successfully wrote data for league_id: {league_id} to {file_path}")
        return {"message": "H2H league written successfully"}

    except Exception as e:
        logger.error(f"Failed to write data for league_id: {league_id}. Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
