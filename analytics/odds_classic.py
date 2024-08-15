import pandas as pd
import numpy as np


# Function to calculate required metrics
def _calculate_metrics(past_seasons):
    if len(past_seasons) == 0:
        return pd.Series({
            'maximum_rank': None,
            'maximum_total_points': None,
            'best_two_seasons_rank': None,
            'best_two_seasons_points': None,
            'minimum_rank': None,
            'minimum_total_points': None,
            'number_of_past_seasons': 0,
            'moving_total_point_variance': None,
            'moving_total_point_average': None,
            'moving_rank_variance': None,
            'moving_rank_average': None,
        })
    
    one_season_penalty_multiplier = 1.25
    if len(past_seasons) == 1:
        return pd.Series({
            'maximum_rank': past_seasons[0]['rank'] * one_season_penalty_multiplier,
            'maximum_total_points': past_seasons[0]['total_points'] / one_season_penalty_multiplier,
            'best_two_seasons_rank': past_seasons[0]['rank'] * one_season_penalty_multiplier,
            'best_two_seasons_points': past_seasons[0]['total_points'] / one_season_penalty_multiplier,
            'minimum_rank': past_seasons[0]['rank'] * one_season_penalty_multiplier,
            'minimum_total_points': past_seasons[0]['total_points'] / one_season_penalty_multiplier,
            'number_of_past_seasons': 1,
            'moving_total_point_variance': None,
            'moving_total_point_average': past_seasons[0]['total_points'] / one_season_penalty_multiplier,
            'moving_rank_variance': None,
            'moving_rank_average': past_seasons[0]['rank'] * one_season_penalty_multiplier,
        })

    past_seasons = sorted(past_seasons, key=lambda x: int(x['season_name'][-2:]))
    ranks = [season['rank'] for season in past_seasons]
    total_points = [season['total_points'] for season in past_seasons]

    if len(total_points) < 2:
        moving_total_point_variance = pd.Series(total_points).std()
        moving_total_point_average = pd.Series(total_points).mean()
        best_two_seasons_points = pd.Series(total_points).mean()
    else:
        moving_total_point_variance = pd.Series(total_points).rolling(window=2).std().mean()
        moving_total_point_average = pd.Series(total_points).rolling(window=2).mean().mean()
        best_two_seasons_points = pd.Series(sorted(total_points, reverse=True)[:2]).mean()

    # Compute moving_rank_variance and moving_rank_average
    if len(ranks) < 2:
        moving_rank_variance = pd.Series(ranks).std()
        moving_rank_average = pd.Series(ranks).mean()
        best_two_seasons_rank = pd.Series(ranks).mean()
    else:
        moving_rank_variance = pd.Series(ranks).rolling(window=2).std().mean()
        moving_rank_average = pd.Series(ranks).rolling(window=2).mean().mean()
        best_two_seasons_rank = pd.Series(sorted(ranks)[:2]).mean()

    return pd.Series({
        'maximum_rank': max(ranks),
        'maximum_total_points': max(total_points),
        'best_two_seasons_rank': best_two_seasons_rank,
        'best_two_seasons_points': best_two_seasons_points,
        'minimum_rank': min(ranks),
        'minimum_total_points': min(total_points),
        'number_of_past_seasons': len(past_seasons),
        'moving_total_point_variance': moving_total_point_variance,
        'moving_total_point_average': moving_total_point_average,
        'moving_rank_variance': moving_rank_variance,
        'moving_rank_average': moving_rank_average,
    })


# Function to calculate percentile ranks based on specified order
def calculate_percentile_ranks(df, column, ascending=True, na_option='bottom'):
    return df[column].rank(pct=True, ascending=ascending, na_option=na_option)


def calculate_metrics(player_histories_df: pd.DataFrame, parsed_players_df: pd.DataFrame) -> pd.DataFrame:
    # Calculate metrics for each player's history
    player_histories_metrics = player_histories_df['past'].apply(_calculate_metrics)

    # Combine the metrics with the original player_histories DataFrame
    player_histories_df = pd.concat([player_histories_df['entry_id'], player_histories_metrics], axis=1)

    # Join with the parsed_players DataFrame on entry_id
    result = pd.merge(parsed_players_df, player_histories_df, on='entry_id', how='inner')

    # Calculate percentile ranks for each feature
    result['percentile_maximum_rank'] = 1.0 - calculate_percentile_ranks(result, 'maximum_rank')
    result['percentile_maximum_total_points'] = calculate_percentile_ranks(result, 'maximum_total_points', na_option='top')
    result['percentile_best_two_seasons_rank'] = 1.0 - calculate_percentile_ranks(result, 'best_two_seasons_rank')
    result['percentile_best_two_seasons_points'] = calculate_percentile_ranks(result, 'best_two_seasons_points', na_option='top')
    result['percentile_minimum_rank'] = 1.0 - calculate_percentile_ranks(result, 'minimum_rank')
    result['percentile_minimum_total_points'] = calculate_percentile_ranks(result, 'minimum_total_points', na_option='top')
    result['percentile_number_of_past_seasons'] = calculate_percentile_ranks(result, 'number_of_past_seasons')
    result['percentile_moving_total_point_variance'] = 1.0 - calculate_percentile_ranks(result, 'moving_total_point_variance')
    result['percentile_moving_total_point_average'] = calculate_percentile_ranks(result, 'moving_total_point_average', na_option='top')
    result['percentile_moving_rank_variance'] = 1.0 - calculate_percentile_ranks(result, 'moving_rank_variance')
    result['percentile_moving_rank_average'] = 1.0 - calculate_percentile_ranks(result, 'moving_rank_average')

    return result


def weighted_manhattan_distance(data: pd.DataFrame, cols: list, ideal_rank: np.ndarray, weights: np.ndarray) -> pd.Series:
    """
    Compute the weighted Manhattan distance between each row in the DataFrame and the ideal rank.
    
    :param data: DataFrame containing the data. 
    :param cols: List of column names to be considered for computing the distance.
    :param ideal_rank: 1D numpy array or list representing the ideal rank for comparison.
    :param weights: 1D numpy array or list containing the weights for each dimension.
    
    :return: A pandas Series containing the weighted Manhattan distance for each row.
    """
    
    # Ensure ideal_rank and weights are numpy arrays
    ideal_rank = np.array(ideal_rank)
    weights = np.array(weights)
    
    # Subset the DataFrame to include only the specified columns
    data_subset = data[cols]
    
    # Define a function to calculate weighted Manhattan distance for a single row
    def row_weighted_manhattan(row):
        abs_diff = np.abs(row - ideal_rank)
        weighted_diff = abs_diff * weights
        return weighted_diff.sum()
    
    # Apply the function row-wise
    weighted_manhattan_distances = data_subset.apply(row_weighted_manhattan, axis=1)
    
    return weighted_manhattan_distances

def distances_to_probabilities(df, column_name):
    """
    Converts a column of distances in a pandas DataFrame to probabilities using the softmax function.
    
    Parameters:
    df (pandas.DataFrame): The input DataFrame.
    column_name (str): The name of the column containing distances.
    
    Returns:
    pandas.Series: A pandas Series with the same index as the input DataFrame containing the probabilities.
    """
    distances = df[column_name]
    
    # Calculate the negative distances
    neg_distances = -distances
    
    # Apply the softmax function
    exp_neg_distances = np.exp(neg_distances)
    probabilities = exp_neg_distances / exp_neg_distances.sum()
    
    return probabilities

def calculate_odds(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute the odds of winning based
    on the weighted manhattan distance.
    """
    # Extract columns that have 'percentile' in their name
    percentile_columns = [col for col in df.columns if 'percentile' in col]

    weights_dict = {
        'percentile_maximum_rank': 0.05,
        'percentile_maximum_total_points': 5,
        'percentile_best_two_seasons_rank': 5,
        'percentile_best_two_seasons_points': 5,
        'percentile_minimum_rank': 2,
        'percentile_minimum_total_points': 0.25,
        'percentile_number_of_past_seasons': 0.25,
        'percentile_moving_total_point_variance': 0.5,
        'percentile_moving_total_point_average': 2,
        'percentile_moving_rank_variance': 0.5,
        'percentile_moving_rank_average': 2
    }

    ideal_vals_dict = {
        'percentile_maximum_rank': 1,
        'percentile_maximum_total_points': 1,
        'percentile_best_two_seasons_rank': 1,
        'percentile_best_two_seasons_points': 1,
        'percentile_minimum_rank': 1,
        'percentile_minimum_total_points': 1,
        'percentile_number_of_past_seasons': 1,
        'percentile_moving_total_point_variance': 1,
        'percentile_moving_total_point_average': 1,
        'percentile_moving_rank_variance': 1,
        'percentile_moving_rank_average': 1
    }

    weights = [weights_dict[col] for col in percentile_columns]

    # Create a vector with ideal values
    ideal_vector = [ideal_vals_dict[col] for col in percentile_columns]

    # Step 1: Compute weighted manhattan distance between the ideal (ultimate champion) vector of percentile ranks and each player
    df['weighted_manhattan_distance'] = weighted_manhattan_distance(df, percentile_columns, ideal_vector, weights)

    # Step 2: Convert the distances to probabilities (likelihood of winning inversely proportional to distance)
    df['probability_of_winning'] = distances_to_probabilities(df, 'weighted_manhattan_distance')

    # Step 3: Calculate the odds of winning based on the probabilities
    df['odds'] = 1 / (df['probability_of_winning'] / (1 - df['probability_of_winning']))

    # Select specific columns including the calculated odds
    df_selected = df[['entry_id', 'player_first_name', 'player_last_name', 'weighted_manhattan_distance', 'probability_of_winning', 'odds']]

    return df_selected.sort_values('odds')

