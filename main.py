import click
import pandas as pd
from utils import format_datatypes, calculate_llm_cost, round_to_nearest_minute, generate_token_distribution, find_peak_token_throughput

@click.command()
@click.argument('input_csv', type=click.Path(exists=True))
def process_data(input_csv):
    # Load the dataset
    df = pd.read_csv(input_csv)

    # Change object variables to correct format
    df = format_datatypes(df)

    # Calculate the LLM cost per flow type and 90th percentile
    llm_cost_90th_percentile = calculate_llm_cost(df)
    print("\n90th Percentile LLM Cost per Flow Type:")
    print(llm_cost_90th_percentile)

    # Round timestamps to the nearest minute
    df['created_at_rounded'] = df['created_at'].apply(round_to_nearest_minute)
    df['finished_at_rounded'] = df['finished_at'].apply(round_to_nearest_minute)

    # Remove rows with any NaT values
    df_cleaned = df.dropna()

    # Generate token distribution
    token_distribution_df = generate_token_distribution(df_cleaned)

    # Find peak minute for token throughput
    peak_minute = find_peak_token_throughput(token_distribution_df)
    print(f"\nThe peak token throughput occurs at {peak_minute['minute']} with {peak_minute['tokens']} tokens.")

if __name__ == '__main__':
    process_data()
