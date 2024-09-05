# Account Manager

A simple transaction processing engine implemented in Rust.

## Description

This project implements a transaction processing system that handles various types of financial transactions including
deposits, withdrawals, disputes, resolves, and chargebacks. It reads transaction data from a CSV file, processes the
transactions, and outputs the final state of client accounts as a CSV.

## Getting Started

### Prerequisites

- Rust programming language (latest stable version)
- Cargo (Rust's package manager)

### Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/transaction-engine.git
   cd account-manager
   ```

2. Build the project:
   ```
   cargo build --release
   ```

### Usage

Run the program with an input CSV file:

```
cargo run -- transactions.csv > accounts.csv
```

The program will process the transactions in the input file and output the final account states to stdout, which you can
redirect to a file as shown above.

## Testing

Run the unit tests with:

```
cargo test
```

The test suite includes various scenarios to ensure correct behavior for different transaction types and edge cases.

## Assumptions

- All transaction for lock client account will be ignored.
- If the transaction amount exceeds the available or held funds (depending on the transaction type), the transaction
  will be ignored.
- Chargeback and resolve can only be processed once.

## Libraries Used

- `serde` for serialization and deserialization
- `csv` for reading and writing CSV files
- `rust_decimal` for precise decimal arithmetic
