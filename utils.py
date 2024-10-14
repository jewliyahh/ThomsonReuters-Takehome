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

def round_to_nearest_minute(dt):
    """Rounds a datetime object to the nearest minute"""
    seconds = dt.second + dt.microsecond / 1e6
    if seconds >= 30:
        dt += datetime.timedelta(minutes=1)
    return dt.replace(second=0, microsecond=0)

def generate_token_distribution(df):
    """Generate token distribution for each flow based on time range and token rate"""
    df = df.copy()
    df['minutes'] = ((df['finished_at_rounded'] - df['created_at_rounded']).dt.total_seconds() // 60).astype(int)

    minutes = []
    tokens = []

    for _, row in df.iterrows():
        minute_range = pd.date_range(start=row['created_at_rounded'], periods=row['minutes'], freq='min')
        tokens_per_minute = (row['tokens_in'] + row['tokens_out']) / row['minutes'] if row['minutes'] > 0 else 0

        minutes.extend(minute_range)
        tokens.extend([tokens_per_minute] * len(minute_range))

    minute_records = pd.DataFrame({'minute': minutes, 'tokens': tokens})
    return minute_records

def find_peak_token_throughput(token_distribution_df):
    """Find the minute with the peak token throughput"""
    minute_aggregated = token_distribution_df.groupby('minute')['tokens'].sum().reset_index()
    peak_minute = minute_aggregated.loc[minute_aggregated['tokens'].idxmax()]
    return peak_minute
