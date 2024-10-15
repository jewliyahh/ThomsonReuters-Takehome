import pandas as pd
import datetime

GPT_4_PRICE_PER_1M_TOKENS_INPUT = 2.50  # USD per 1M input tokens
GPT_4_PRICE_PER_1M_TOKENS_OUTPUT = 10.00  # USD per 1M output tokens

def format_datatypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts columns in the DataFrame to appropriate data types such as strings and datetime.
    
    Parameters:
    -----------
    df : pd.DataFrame
        A DataFrame with object-type columns.

    Returns:
    --------
    pd.DataFrame
        The DataFrame with correctly formatted data types.
    """
    df['flow_id'] = df['flow_id'].astype('string')
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce', utc=True)
    df['finished_at'] = pd.to_datetime(df['finished_at'], utc=True)
    df['status'] = df['status'].astype('string')
    df['type'] = df['type'].astype('string')
    df['region'] = df['region'].astype('string')
    df['user_id'] = df['user_id'].astype('string')
    df['org_id'] = df['org_id'].astype('string')
    
    df['tokens_in'] = df['tokens_in'].fillna(0).astype('int64')
    df['tokens_out'] = df['tokens_out'].fillna(0).astype('int64')
    df['llm_calls'] = df['llm_calls'].fillna(0).astype('int64')

    return df


def calculate_llm_cost(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the LLM cost based on input and output token counts and returns the 90th percentile cost by flow type.
    
    Parameters:
    -----------
    df : pd.DataFrame
        The DataFrame containing token data and flow types.

    Returns:
    --------
    pd.DataFrame
        A DataFrame containing the 90th percentile LLM cost per flow type.
    """
    df['llm_cost'] = (df['tokens_in'] / 1_000_000 * GPT_4_PRICE_PER_1M_TOKENS_INPUT) + \
                     (df['tokens_out'] / 1_000_000 * GPT_4_PRICE_PER_1M_TOKENS_OUTPUT)
    llm_cost_90th_percentile = df.groupby('type')['llm_cost'].quantile(0.90).reset_index()
    llm_cost_90th_percentile.columns = ['flow_type', '90th_percentile_llm_cost']
    return llm_cost_90th_percentile


def round_to_nearest_second(dt: pd.Timestamp) -> pd.Timestamp:
    """
    Rounds a datetime object to the nearest second.
    
    Parameters:
    -----------
    dt : pd.Timestamp
        A pandas datetime object.

    Returns:
    --------
    pd.Timestamp
        The datetime rounded to the nearest second.
    """
    fractional_seconds = dt.microsecond / 1e6
    if fractional_seconds >= 0.5:
        dt += datetime.timedelta(seconds=1)
    
    return dt.replace(microsecond=0)


def generate_token_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generates a token distribution over time for each flow in the DataFrame.
    
    Parameters:
    -----------
    df : pd.DataFrame
        The cleaned DataFrame with flow data and rounded timestamps.

    Returns:
    --------
    pd.DataFrame
        A DataFrame containing second-level token distribution with timestamps and token counts.
    """
    df = df.copy()
    df['duration_seconds'] = (df['finished_at_rounded'] - df['created_at_rounded']).dt.total_seconds()
    df['total_tokens'] = df['tokens_in'] + df['tokens_out']
    df['tokens_per_second'] = df['total_tokens'] / df['duration_seconds'].replace(0, 1)  # Avoid division by zero
    
    data_dict = df.to_dict(orient='records')
    minutes, tokens = [], []

    for record in data_dict:
        second_range = pd.date_range(start=record['created_at_rounded'], 
                                      end=record['finished_at_rounded'], freq='S')
        num_seconds = len(second_range)
        minutes.extend(second_range)
        tokens.extend([record['tokens_per_second']] * num_seconds)

    token_distribution_df = pd.DataFrame({'second': minutes, 'tokens': tokens})
    return token_distribution_df


def find_peak_token_throughput(token_distribution_df: pd.DataFrame) -> pd.Series:
    """
    Finds the minute with the peak token throughput based on the distribution.
    
    Parameters:
    -----------
    token_distribution_df : pd.DataFrame
        A DataFrame containing second-level token distribution.

    Returns:
    --------
    pd.Series
        A Series representing the minute with the highest token throughput and the token count.
    """
    token_distribution_df['minute'] = token_distribution_df['second'].dt.floor('T')
    minute_records = token_distribution_df.groupby('minute')['tokens'].sum().reset_index()
    max_tokens_minute = minute_records.loc[minute_records['tokens'].idxmax()]
    
    return max_tokens_minute

