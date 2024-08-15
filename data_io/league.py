from .utils import fetch_data

def get_classic_league_data(league_id: str):
    base_url = f"https://fantasy.premierleague.com/api/leagues-classic/{league_id}/standings/"

    params = {
        "page_standings": 1,
        "page_new_entries": 1,
        # Add more parameters if needed
    }

    output = []

    has_next = True
    while has_next:
        has_next_entries = True
        while has_next_entries:
            url_appendix = "&".join(["?{key}=" + str(params[key]) for key in params])
            data = fetch_data(base_url + url_appendix, params)
            has_next = data.get('has_next', False)
            has_next_entries = data.get('new_entries', False)
            if has_next_entries:
                has_next_entries = data['new_entries'].get('has_next', False)
            
            # Store the data in the output list
            output.append(data)
            params['page_new_entries'] += 1
        
        # Update params if necessary (e.g., to fetch the next page)
        params['page_standings'] += 1
    
    return output


def get_h2h_league_data(league_id: str):
    base_url = f"https://fantasy.premierleague.com/api/leagues-h2h-matches/league/{league_id}/"
    pass
