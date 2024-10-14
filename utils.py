import pandas as pd
import datetime

GPT_4_PRICE_PER_1M_TOKENS_INPUT = 2.50  # USD per 1M input tokens
GPT_4_PRICE_PER_1M_TOKENS_OUTPUT = 10.00  # USD per 1M output tokens

def format_datatypes(df): 
    """Establish the right datatype

    Input: Panda DF with object datatypes 
    Ouput: Panda DF with datatypes 
    """
    df['flow_id'] = df['flow_id'].astype('string')
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce', utc=True)
    df['finished_at'] = pd.to_datetime(df['finished_at'], utc=True)
    df['status'] = df['status'].astype('string')
    df['type'] = df['type'].astype('string') 
    df['region'] = df['region'].astype('string')  
    df['user_id'] = df['user_id'].astype('string')  
    df['org_id'] = df['org_id'].astype('string')  

    
    df['tokens_in'] = df['tokens_in'].fillna(0)
    df['tokens_in'] = df['tokens_in'].astype('int64')

    df['tokens_out'] = df['tokens_out'].fillna(0)
    df['tokens_out'] = df['tokens_out'].astype('int64')

    df['llm_calls'] = df['llm_calls'].fillna(0)
    df['llm_calls'] = df['llm_calls'].astype('int64')

    return df 


def calculate_llm_cost(df):
    """Calculate LLM cost based on token counts"""
    df['llm_cost'] = (df['tokens_in'] / 1_000_000 * GPT_4_PRICE_PER_1M_TOKENS_INPUT) + \
                     (df['tokens_out'] / 1_000_000 * GPT_4_PRICE_PER_1M_TOKENS_OUTPUT)
    llm_cost_90th_percentile = df.groupby('type')['llm_cost'].quantile(0.90).reset_index()
    llm_cost_90th_percentile.columns = ['flow_type', '90th_percentile_llm_cost']
    return llm_cost_90th_percentile

def round_to_nearest_second(dt):
    """Rounds a datetime object to the nearest second."""
    
    # Calculate the fractional seconds
    fractional_seconds = dt.microsecond / 1e6
    
    # If the fractional seconds are 0.5 or more, increment the second
    if fractional_seconds >= 0.5:
        dt += datetime.timedelta(seconds=1)
    
    return dt.replace(microsecond=0)

def generate_token_distribution(df):
    # Create a copy of the DataFrame to avoid SettingWithCopyWarning
    df = df.copy()

    # Get duration of flow in seconds
    df['duration_seconds'] = (df['finished_at_rounded'] - df['created_at_rounded']).dt.total_seconds()

    # Calculate total tokens and tokens per second
    df['total_tokens'] = df['tokens_in'] + df['tokens_out']
    df['tokens_per_second'] = df['total_tokens'] / df['duration_seconds'].replace(0, 1)  # Avoid division by zero

    # Convert DataFrame to a list of dictionaries
    data_dict = df.to_dict(orient='records')

    minutes = []
    tokens = []

    # Iterate over each record in the data_dict
    for record in data_dict:
        # Create a second-level range from created_at to finished_at
        second_range = pd.date_range(start=record['created_at_rounded'], 
                                      end=record['finished_at_rounded'], 
                                      freq='S')  # 'S' for seconds
        
        # Calculate the number of seconds in this range
        num_seconds = len(second_range)

        # Extend the minutes and tokens lists
        minutes.extend(second_range)
        tokens.extend([record['tokens_per_second']] * num_seconds)

    # Create a DataFrame for the second-level data
    token_distribution_df = pd.DataFrame({'second': minutes, 'tokens': tokens})

    return token_distribution_df

def find_peak_token_throughput(token_distribution_df):
    """Find the minute with the peak token throughput"""
    
    # Group by minute and sum tokens
    token_distribution_df['minute'] = token_distribution_df['second'].dt.floor('T')  # Get the minute part
    minute_records = token_distribution_df.groupby('minute')['tokens'].sum().reset_index()

    # Find the minute with the maximum tokens
    max_tokens_minute = minute_records.loc[minute_records['tokens'].idxmax()]
    return max_tokens_minute
