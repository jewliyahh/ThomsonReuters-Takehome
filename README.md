# LLM Cost Analysis CLI

This CLI tool processes a CSV dataset containing details about various flows, including their creation and completion times, LLM token usage, and flow types. It performs the following operations:

1. **Calculates the 90th percentile of LLM cost per flow type**.
2. **Rounds the `created_at` and `finished_at` timestamps to the nearest minute**.
3. **Generates a minute-by-minute token distribution and finds the peak token throughput**.

## Dataset Schema

The dataset should be in CSV format with the following schema:

| Column Name    | Data Type |
|----------------|-----------|
| `flow_id`      | STRING    |
| `created_at`   | TIMESTAMP (UTC) |
| `finished_at`  | TIMESTAMP (UTC) |
| `status`       | STRING    |
| `type`         | STRING    |
| `region`       | STRING    |
| `user_id`      | STRING    |
| `org_id`       | STRING    |
| `tokens_in`    | INTEGER   |
| `tokens_out`   | INTEGER   |
| `llm_calls`    | INTEGER   |

## Features

- **LLM Cost Calculation**: Calculates the LLM cost per flow type using predefined prices for input and output tokens:
  - Input Token Price: $2.50 per 1M tokens
  - Output Token Price: $10.00 per 1M tokens
- **90th Percentile Calculation**: Computes the 90th percentile of LLM cost per flow type.
- **Rounding Timestamps**: Rounds the `created_at` and `finished_at` columns to the nearest minute.
- **Token Distribution**: Generates a minute-by-minute token distribution and identifies the peak minute for token throughput.

## Installation

### Prerequisites

- Python 3.11
- pip (Python package manager)

### Clone the repository 

```
git clone 
```

### Install dependencies

After cloning the repository, install the necessary dependencies using `pip`:

```bash
pip install -r requirements.txt
```

## Usage

1. **Prepare your CSV file**: Ensure your dataset is in the correct format as described in the schema above.

2. **Run the CLI**:

```bash
python main.py <your csv file>
```

This will execute the analysis on the dataset and display the results, including the 90th percentile LLM cost and peak token throughput minute.

## Example Output

```
90th percentile LLM cost per flow type:
+----------------+-------------------------+
| flow_type      | 90th_percentile_llm_cost |
+----------------+-------------------------+
| type_1         | 15.34                    |
| type_2         | 12.67                    |
| ...            | ...                      |
+----------------+-------------------------+

Peak token throughput occurs at 2024-01-01 12:34:00 with 5,678 tokens.
```

## File Structure

```
├── main.py                 # Main Python script for CLI
├── requirements.txt        # Python package dependencies
├── README.md               # Project documentation
├── exercise_data.csv       # Example dataset 
├── exploration.ipynb       # Exploration of the dataset and investigation of how to implement the tasks
```

## Contributing

Feel free to submit issues or contribute to the project by forking the repository and creating a pull request.

## License

None, this is takehome assignment 