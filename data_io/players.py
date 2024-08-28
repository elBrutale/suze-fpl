import requests
from .utils import fetch_data


def extract_player_data(data):
    # Extract the new entries
    new_entries = data.get('new_entries', {}).get('results', [])
    
    # Create a list to store parsed entries
    parsed_entries = []
    added_entries = set()
    
    for entry in new_entries:
        parsed_entry = {
            "entry_id": entry.get("entry"),
            "entry_name": entry.get("entry_name"),
            "joined_time": entry.get("joined_time"),
            "player_first_name": entry.get("player_first_name"),
            "player_last_name": entry.get("player_last_name")
        }
        if parsed_entry['entry_id'] not in added_entries:
            added_entries.add(parsed_entry['entry_id'])
            parsed_entries.append(parsed_entry)

    # Extract the standings results
    standings = data.get('standings', {}).get('results', [])
    
    for entry in standings:
        parsed_entry = {
            "entry_id": entry.get("entry"),
            "entry_name": entry.get("entry_name"),
            "joined_time": None,  # Set to None as it's not present in standings
            "player_first_name": None,  # Set to None as it's not present in standings
            "player_last_name": None  # Set to None as it's not present in standings
        }
        if parsed_entry['entry_id'] not in added_entries:
            added_entries.add(parsed_entry['entry_id'])
            parsed_entries.append(parsed_entry)
    
    return parsed_entries


def get_player_history(entry_id: str):
    # Base URL for sending requests
    base_url = "https://fantasy.premierleague.com/api/entry/{team_id}/history/"
    # Construct the URL with the entry_id
    url = base_url.format(team_id=entry_id)
    
    try:
        # Send the request to the API
        result = fetch_data(url)
        result['entry_id'] = entry_id
        print(f"Successfully processed entry_id: {entry_id}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to process entry_id: {entry_id} - {e}")
    
    return result

def get_transfer_history(entry_id: str):
    # Base URL for sending requests
    base_url = "https://fantasy.premierleague.com/api/entry/{team_id}/transfers/"
    # Construct the URL with the entry_id
    url = base_url.format(team_id=entry_id)
    
    try:
        # Send the request to the API
        result = fetch_data(url)
        print(f"Successfully fetched transfers for entry_id: {entry_id}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch transfers for entry_id: {entry_id} - {e}")
    
    return result

def get_picks_history(gw_number: str, entry_id: str):
    # Base URL for sending requests
    url = f"https://fantasy.premierleague.com/api/entry/{entry_id}/event/{gw_number}/picks/"
    
    try:
        # Send the request to the API
        result = fetch_data(url)
        result['entry_id'] = entry_id
        print(f"Successfully fetched picks for entry_id: {entry_id}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch picks for entry_id: {entry_id} - {e}")
    
    return result
