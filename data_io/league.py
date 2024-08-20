from .utils import fetch_data

LEAGUE_FIELDNAMES = ['id', 'name', 'created', 'closed', 'max_entries', 'league_type', 
                        'scoring', 'admin_entry', 'start_event', 'code_privacy', 'has_cup', 'cup_league', 'rank']

H2H_LEAGUE_FIELDNAMES = ['id', 'name', 'created', 'closed', 'max_entries', 'league_type', 
                        'scoring', 'admin_entry', 'start_event', 'code_privacy', 'has_cup', 'cup_league', 'rank', 'ko_rounds']

STANDINGS_FIELDNAMES = ['id', 'division', 'entry', 'player_name', 'rank', 
                                    'last_rank', 'rank_sort', 'total', 'entry_name', 
                                    'matches_played', 'matches_won', 'matches_drawn', 
                                    'matches_lost', 'points_for', 'timestamp_requested', 'league_id']


def get_league_data(league_id: str, league_type='leagues-classic'):
    base_url = f"https://fantasy.premierleague.com/api/{league_type}/{league_id}/standings/"

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
            has_next_entries = data.get('new_entries', False)
            if has_next_entries:
                has_next_entries = data['new_entries'].get('has_next', False)
            if has_next_entries:
                params['page_new_entries'] += 1
            # Store the data in the output list
            output.append(data)
        
        standings = data.get('standings', {})
        has_next = standings.get('has_next', False)
        # Update params if necessary (e.g., to fetch the next page)
        if has_next:
            params['page_standings'] += 1
    
    return output


def get_h2h_matches(league_id: str):
    base_url = f"https://fantasy.premierleague.com/api/leagues-h2h-matches/league/{league_id}/"
    params = {
        "page": 1,
        # Add more parameters if needed
    }

    output = []

    has_next = True
    while has_next:
        url_appendix = "&".join(["?{key}=" + str(params[key]) for key in params])
        data = fetch_data(base_url + url_appendix, params)
        # Store the data in the output list
        output.append(data)
        has_next = data.get('has_next', False)
        # Update params if necessary (e.g., to fetch the next page)
        if has_next:
            params['page'] += 1
    
    return output

