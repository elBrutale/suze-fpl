from fastapi import FastAPI, HTTPException
from data_io.league import LEAGUE_FIELDNAMES, STANDINGS_FIELDNAMES, H2H_LEAGUE_FIELDNAMES
from data_io.league import get_league_data, get_h2h_matches, get_fpl_master_data
from data_io.players import extract_player_data, get_player_history, get_transfer_history, get_picks_history

from analytics.utils import jsonl_to_df, read_dataframe
from analytics.odds_classic import calculate_metrics, calculate_odds

from datetime import datetime

import os
import csv
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

import csv
from datetime import datetime

import csv
from datetime import datetime

def filter_max_timestamp_and_map_id_to_webname(csv_file):
    # Dictionary to hold the max timestamp entry for each player
    max_timestamp_entries = {}

    # Open the CSV file
    with open(csv_file, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        
        for row in reader:
            player_id = int(row['id'])
            web_name = row['web_name']
            current_timestamp = row['current_timestamp']

            # Parse the timestamp
            timestamp = datetime.fromisoformat(current_timestamp.replace("Z", "+00:00"))

            # Check if this player is already in the dictionary
            if player_id in max_timestamp_entries:
                # Compare timestamps and keep the entry with the max timestamp
                existing_timestamp = max_timestamp_entries[player_id]['current_timestamp']
                if timestamp > existing_timestamp:
                    max_timestamp_entries[player_id] = {'current_timestamp': timestamp, 'web_name': web_name}
            else:
                # Add new entry to the dictionary
                max_timestamp_entries[player_id] = {'current_timestamp': timestamp, 'web_name': web_name}

    # Create a mapping of id to web_name with the max timestamp
    id_to_webname = {player_id: data['web_name'] for player_id, data in max_timestamp_entries.items()}

    return id_to_webname


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
        existing_entry_ids = set()

        # Extract player data and write to the output file
        with open(input_file_path, 'r') as infile, open(output_file_path, 'w') as outfile:
            for line in infile:
                json_data = json.loads(line)
                players = extract_player_data(json_data)
                for player in players:
                    if player['entry_id'] not in existing_entry_ids:
                        existing_entry_ids.add(player['entry_id'])
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
                for gw in player_history:
                    outfile.write(json.dumps(gw) + '\n')
                logger.debug(f"Written transfer history data for entry_id: {player['entry_id']}")

        logger.info(f"Successfully wrote transfer history data to {output_file_path}")
        return {"message": "Transfer history data written successfully"}

    except Exception as e:
        logger.error(f"Failed to write transfer history data. Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    

@app.get("/suze/static-data")
async def get_static_data():
    try:
        logger.info("Received request toget all fpl player data")

        # Define file paths
        output_file_path = os.path.join("data", "static_data.json")
        output_fpl_players_path = os.path.join("data", "fpl_players_data.csv")

        # Extract picks history and write to the output file
        master_data = get_fpl_master_data()
        with open(output_file_path, 'w') as outfile:
            outfile.write(json.dumps(master_data) + '\n')
        
        logger.info(f"Successfully wrote picks history data to {output_file_path}")

        # Check if the CSV file already exists
        file_exists = os.path.exists(output_fpl_players_path)

        with open(output_fpl_players_path, 'a', newline='') as csvfile:
            current_timestamp = datetime.now().isoformat()
            players = master_data['elements']
            # Define the fieldnames (i.e., CSV column headers)
            fieldnames = list(players[0].keys()) + ['current_timestamp']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            # If the file doesn't exist, write the header
            if not file_exists:
                writer.writeheader()
            for player in players:
                player['current_timestamp'] = current_timestamp
                writer.writerow(player)

        logger.info(f"Successfully wrote FPL player data to {output_fpl_players_path}")
        return {"message": "Static data written successfully"}

    except Exception as e:
        logger.error(f"Failed to write static data. Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    

@app.get("/suze/classic-league/picks_history/{gw_number}")
async def write_picks_history_file(gw_number: str):
    try:
        logger.info("Received request to extract and write picks history data")

        # Define file paths
        players_file_path = os.path.join("data", "players.jsonl")
        fpl_players_path = os.path.join("data", "fpl_players_data.csv")
        output_file_path = os.path.join("data", "picks_history.jsonl")

        player_id_to_name = filter_max_timestamp_and_map_id_to_webname(fpl_players_path)

        # Extract picks history and write to the output file
        with open(players_file_path, 'r') as inplayers, open(output_file_path, 'a') as outfile:
            for line in inplayers:
                player = json.loads(line)
                picks_history = get_picks_history(gw_number=gw_number, entry_id=player['entry_id'])
                for j in range(len(picks_history["picks"])):
                    picks_history["picks"][j]['player_name'] = player_id_to_name.get(picks_history["picks"][j]['element'], 'Unknown')
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
        league_data = get_league_data(league_id=league_id, league_type='leagues-classic')
        logger.info(f"Successfully retrieved data for league_id: {league_id}")

        # Ensure the data directory exists
        os.makedirs("data", exist_ok=True)

        # Write to the file
        # Append each JSON object to the JSONL file
        jsonl_file_path = os.path.join("data", "classic_league.jsonl")
        with open(jsonl_file_path, 'a') as f:
            for data in league_data:
                f.write(json.dumps(data) + '\n')
                logger.debug(f"Written data to file for league_id: {league_id} - Data: {data}")
        
        logger.info(f"Successfully wrote data for league_id: {league_id} to {jsonl_file_path}")
        
        # Load existing data from CSV into a dictionary with league id as key
        data = {}
        csv_file_path = os.path.join("data", "leagues.csv")
        if os.path.exists(csv_file_path):
            with open(csv_file_path, mode='r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    data[int(row['id'])] = row
        
            logger.info(f"Read in previous league entries from {csv_file_path}")
            logger.info(f"There are {len(data)} entries in the league file.")
        
        # Determine the starting line number
        start_line = 0
        state_file_path = os.path.join("data", "state.txt")
        if os.path.exists(state_file_path):
            with open(state_file_path, 'r', encoding='utf-8') as statefile:
                start_line = int(statefile.read().strip())
            logger.info(f"There are {start_line+1} lines already processed in the JSONL file.")
        
        # Process the JSONL file incrementally
        with open(jsonl_file_path, 'r', encoding='utf-8') as jsonlfile:
            for current_line_number, line in enumerate(jsonlfile):
                if current_line_number < start_line:
                    continue  # Skip already processed lines

                entry = json.loads(line)
                league = entry['league']
                league_id = league['id']
                created_timestamp = datetime.fromisoformat(league['created'].replace('Z', '+00:00'))

                # Check if this league id exists in the data
                if league_id in data:
                    existing_timestamp = datetime.fromisoformat(data[league_id]['created'].replace('Z', '+00:00'))
                    if created_timestamp > existing_timestamp:
                        # Update the entry with the new data
                        data[league_id] = league
                        logger.info(f"Updated {league_id} with latest data.")
                else:
                    # Add a new entry
                    data[league_id] = league
                    logger.info(f"Adding {league_id} information.")

        # Write the updated data back to the CSV file
        with open(csv_file_path, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=LEAGUE_FIELDNAMES)
            writer.writeheader()
            for league_id, league in data.items():
                writer.writerow(league)
        
        logger.info(f"Successfully wrote data to {csv_file_path}")
        
        # Update the state file with the last processed line number
        with open(state_file_path, 'w', encoding='utf-8') as statefile:
            statefile.write(str(current_line_number + 1))  # +1 to store the next starting line
        
        logger.info(f"Updated state file with last processed line number: {current_line_number + 1}")

        return {"message": "Classic league written successfully"}

    except Exception as e:
        logger.error(f"Failed to write data for league_id: {league_id}. Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    
@app.get("/suze/h2h-league/{league_id}")
async def write_h2h_file(league_id: str):
    try:
        logger.info(f"Received request to fetch and write h2h league data for league_id: {league_id}")
        
        # Call your function from get_classic_league to get the data
        league_data = get_league_data(league_id=league_id, league_type='leagues-h2h')
        logger.info(f"Successfully retrieved data for league_id: {league_id}")

        # Ensure the data directory exists
        os.makedirs("data", exist_ok=True)

        # Write to the file
        # Append each JSON object to the JSONL file
        jsonl_file_path = os.path.join("data", "h2h_leagues.jsonl")
        with open(jsonl_file_path, 'a') as f:
            for data in league_data:
                f.write(json.dumps(data) + '\n')
                logger.debug(f"Written data to file for league_id: {league_id} - Data: {data}")
        
        logger.info(f"Successfully wrote data for league_id: {league_id} to {jsonl_file_path}")
        
        # Load existing data from CSV into a dictionary with league id as key
        data = {}
        csv_file_path = os.path.join("data", "h2h_leagues.csv")
        if os.path.exists(csv_file_path):
            with open(csv_file_path, mode='r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    data[int(row['id'])] = row
        
            logger.info(f"Read in previous league entries from {csv_file_path}")
            logger.info(f"There are {len(data)} entries in the league file.")
        
        # Determine the starting line number
        start_line = 0
        state_file_path = os.path.join("data", "state_h2h.txt")
        if os.path.exists(state_file_path):
            with open(state_file_path, 'r', encoding='utf-8') as statefile:
                start_line = int(statefile.read().strip())
            logger.info(f"There are {start_line+1} lines already processed in the JSONL file.")
        
        # Process the JSONL file incrementally
        standings_csv_path = os.path.join("data", "standings_h2h.csv")
        with open(jsonl_file_path, 'r', encoding='utf-8') as jsonlfile:
            for current_line_number, line in enumerate(jsonlfile):
                if current_line_number < start_line:
                    continue  # Skip already processed lines

                entry = json.loads(line)
                league = entry['league']
                league_id = league['id']
                standings = entry.get('standings', {})
                created_timestamp = datetime.fromisoformat(league['created'].replace('Z', '+00:00'))

                # Check if this league id exists in the data
                if league_id in data:
                    existing_timestamp = datetime.fromisoformat(data[league_id]['created'].replace('Z', '+00:00'))
                    if created_timestamp > existing_timestamp:
                        # Update the entry with the new data
                        data[league_id] = league
                        logger.info(f"Updated {league_id} with latest data.")
                else:
                    # Add a new entry
                    data[league_id] = league
                    logger.info(f"Adding {league_id} information.")
                
                if standings:
                    # Get the current timestamp
                    current_time = datetime.now().isoformat()

                    # Check if the standings file exists to determine the write mode
                    write_mode = 'a' if os.path.exists(standings_csv_path) else 'w'

                    # Write the standings data
                    with open(standings_csv_path, mode=write_mode, newline='', encoding='utf-8') as standings_file:
                        writer = csv.DictWriter(standings_file, fieldnames=STANDINGS_FIELDNAMES)

                        # Write the header only if the file is newly created
                        if write_mode == 'w':
                            writer.writeheader()

                        # Loop through the results in the standings data and write each row with the timestamp
                        for result in standings.get("results", []):
                            result['timestamp_requested'] = current_time
                            result['league_id'] = league_id
                            writer.writerow(result)
                    
                    logger.info(f"Successfully wrote standings data to {standings_csv_path}")

        # Write the updated data back to the CSV file
        with open(csv_file_path, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=H2H_LEAGUE_FIELDNAMES)
            writer.writeheader()
            for league_id, league in data.items():
                writer.writerow(league)
        
        logger.info(f"Successfully wrote data to {csv_file_path}")
        
        # Update the state file with the last processed line number
        with open(state_file_path, 'w', encoding='utf-8') as statefile:
            statefile.write(str(current_line_number + 1))  # +1 to store the next starting line
        
        logger.info(f"Updated state file with last processed line number: {current_line_number + 1}")

        return {"message": "H2H league written successfully"}

    except Exception as e:
        logger.error(f"Failed to write data for league_id: {league_id}. Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")


@app.get("/suze/h2h-league/{league_id}/matches")
async def write_h2h_file(league_id: str):
    try:
        logger.info(f"Received request to fetch and write h2h league matches for league_id: {league_id}")
        
        # Call your function from get_classic_league to get the data
        match_pages = get_h2h_matches(league_id=league_id)
        logger.info(f"Successfully retrieved data for league_id: {league_id}")

        # Ensure the data directory exists
        os.makedirs("data", exist_ok=True)

        matches_csv_path = os.path.join("data", "matches_h2h.csv")

        # Get the current timestamp
        timestamp_requested = datetime.now().isoformat()

        # Check if the CSV file already exists
        file_exists = os.path.exists(matches_csv_path)

        # Open the CSV file in append mode
        with open(matches_csv_path, mode='a', newline='') as file:
            fieldnames = ['id', 'entry_1_entry', 'entry_1_name', 'entry_1_player_name', 
                        'entry_1_points', 'entry_1_win', 'entry_1_draw', 'entry_1_loss', 
                        'entry_1_total', 'entry_2_entry', 'entry_2_name', 'entry_2_player_name', 
                        'entry_2_points', 'entry_2_win', 'entry_2_draw', 'entry_2_loss', 
                        'entry_2_total', 'is_knockout', 'league', 'winner', 'seed_value', 
                        'event', 'tiebreak', 'is_bye', 'knockout_name', 'timestamp_requested']

            writer = csv.DictWriter(file, fieldnames=fieldnames)

            # Write the header if the file doesn't exist yet
            if not file_exists:
                writer.writeheader()

            # Read existing entries if file exists
            existing_entries = set()
            if file_exists:
                with open(matches_csv_path, mode='r') as read_file:
                    reader = csv.DictReader(read_file)
                    for row in reader:
                        key = (row['league'], row['event'], row['entry_1_entry'], row['entry_2_entry'])
                        existing_entries.add(key)

            # Write each match to the CSV if it doesn't already exist
            for matches in match_pages:
                for match in matches['results']:
                    key = (str(match['league']), str(match['event']), str(match['entry_1_entry']), str(match['entry_2_entry']))
                    # append new gws
                    if key not in existing_entries:
                        empty_entry_cols = [
                            "entry_1_points", "entry_1_win", "entry_1_draw", "entry_1_loss", "entry_1_total",
                            "entry_2_points", "entry_2_win", "entry_2_draw", "entry_2_loss", "entry_2_total"
                        ]
                        to_write = False
                        for col in empty_entry_cols:
                            if str(match[col]) != '0':
                                to_write = True
                                break
                        if to_write:
                            match['timestamp_requested'] = timestamp_requested
                            writer.writerow(match)

            return {"message": "H2H league matches written successfully"}

    except Exception as e:
        logger.error(f"Failed to write data for league_id: {league_id}. Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    

@app.get("/suze/pregled-kola/{gw_number}")
async def pregled_kola(gw_number: str):
    # Reading the JSONL files
    picks_data = []
    players_data = {}
    picks_file = os.path.join("data", "picks_history.jsonl")
    players_file = os.path.join("data", "players.jsonl")

    gw_number = int(gw_number)

    with open(picks_file, 'r') as file:
        for line in file:
            picks = json.loads(line)
            if picks['entry_history']['event'] == gw_number:
                picks_data.append(picks)

    with open(players_file, 'r') as file:
        for line in file:
            player_info = json.loads(line)
            players_data[player_info['entry_id']] = player_info

    # Initializing variables
    total_players = len(picks_data)
    bank_data = []
    chips_used = {}
    ownership_data = {}
    transfer_in_counts = {}
    transfer_out_counts = {}
    captains = {}
    negative_transfer_points = []
    effective_ownership = {}

    # Processing each player's data
    for pick in picks_data:
        entry_id = pick['entry_id']
        player_name = f"{players_data[entry_id]['player_first_name']} {players_data[entry_id]['player_last_name']}"
        entry_history = pick['entry_history']

        # Bank information
        if entry_history['bank'] > 0:
            bank_data.append((entry_history['bank'], player_name))
        
        # Chips used
        if pick['active_chip']:
            chips_used[player_name] = pick['active_chip']
        
        # Ownership calculation
        for p in pick['picks']:
            if p['element'] not in ownership_data:
                ownership_data[p['element']] = {'count': 0, 'player_name': p['player_name']}
            ownership_data[p['element']]['count'] += 1
        
        # Effective ownership calculation and captains
        for p in pick['picks']:
            if p['is_captain']:
                if p['player_name'] not in effective_ownership:
                    effective_ownership[p['player_name']] = 0
                effective_ownership[p['player_name']] += 2
                captains[p['player_name']] = captains.get(p['player_name'], 0) + 1
            else:
                if p['player_name'] not in effective_ownership:
                    effective_ownership[p['player_name']] = 0
                effective_ownership[p['player_name']] += 1
        
        # Handle transfer ins/outs
        for sub in pick.get('automatic_subs', []):
            transfer_in_counts[sub['element_in']] = transfer_in_counts.get(sub['element_in'], 0) + 1
            transfer_out_counts[sub['element_out']] = transfer_out_counts.get(sub['element_out'], 0) + 1
        
        # Negative transfer points
        if entry_history['event_transfers_cost'] < 0:
            negative_transfer_points.append(player_name)
    
    # Sort and format the output
    n_bank = len(bank_data)
    max_bank_amount, max_bank_player = max(bank_data, default=(0, 'N/A'))
    
    chips_used_str = '\n'.join([f"{name}: {chip}" for name, chip in chips_used.items()])
    ownership_str = '\n'.join([f"{data['player_name']}\n{(data['count'] / total_players) * 100:.2f}% ({data['count']} / {total_players})" 
                               for data in ownership_data.values() if data['count'] / total_players >= 0.5])

    highest_effective_ownership_player = max(effective_ownership, key=effective_ownership.get)
    highest_effective_ownership = effective_ownership[highest_effective_ownership_player]
    element_to_player_name = {}
    for pick in picks_data:
        for p in pick['picks']:
            element_to_player_name[p['element']] = p['player_name']

    most_transferred_in_player = max(transfer_in_counts, key=transfer_in_counts.get, default='N/A')
    most_transferred_out_player = max(transfer_out_counts, key=transfer_out_counts.get, default='N/A')
    most_transferred_in_player_name = element_to_player_name.get(most_transferred_in_player, 'N/A')
    most_transferred_out_player_name = element_to_player_name.get(most_transferred_out_player, 'N/A')

    negative_transfer_points_str = '\n'.join(negative_transfer_points)

    captains_str = '\n'.join([f"{name}: {count / total_players * 100:.2f}%" for name, count in captains.items()])
    
    output = f"""KOLO
{n_bank}/{total_players} igrača ostavilo je para u banci: {max_bank_player} - {max_bank_amount}m £

Iskorišteni chipovi: 
{chips_used_str}

Ownership igrača (>=50%):

{ownership_str}


{highest_effective_ownership_player} effective ownership: {highest_effective_ownership}

TRANSFERI
/

IGRAČ KOJI JE UŠAO U NAJVIŠE EKIPA - {most_transferred_in_player_name}
IGRAČ KOJI JE IZAŠAO IZ NAJVIŠE EKIPA - {most_transferred_out_player_name}

MINUS:
{negative_transfer_points_str}

KAPETANI
{captains_str}
"""
    
    return {"message": output}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
