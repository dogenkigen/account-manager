use csv::{ReaderBuilder, WriterBuilder};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Debug, Deserialize)]
struct Transaction {
    #[serde(rename = "type")]
    tx_type: TransactionType,
    client: u16,
    tx: u32,
    #[serde(default, deserialize_with = "deserialize_decimal")]
    amount: Decimal,
}

fn deserialize_decimal<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<&str> = Option::deserialize(deserializer)?;
    match s {
        Some("") | None => Ok(Decimal::new(0, 4)),
        Some(value) => value.parse::<Decimal>().map_err(serde::de::Error::custom),
    }
}

#[derive(Debug, Serialize)]
struct Account {
    client: u16,
    #[serde(serialize_with = "serialize_decimal")]
    available: Decimal,
    #[serde(serialize_with = "serialize_decimal")]
    held: Decimal,
    #[serde(serialize_with = "serialize_decimal")]
    total: Decimal,
    locked: bool,
}

impl Account {
    fn new(client: u16) -> Self {
        Account {
            client,
            available: Decimal::default(),
            held: Decimal::default(),
            total: Decimal::default(),
            locked: false,
        }
    }
}

fn serialize_decimal<S>(decimal: &Decimal, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if decimal.is_zero() {
        serializer.serialize_i32(0)
    } else {
        serializer.serialize_str(&decimal.to_string())
    }
}

#[derive(Debug)]
struct TransactionDetails {
    amount: Decimal,
    disputed: bool,
}

struct TransactionEngine {
    accounts: HashMap<u16, Account>,
    transactions: HashMap<u32, TransactionDetails>,
}

impl TransactionEngine {
    fn new() -> Self {
        TransactionEngine {
            accounts: HashMap::new(),
            transactions: HashMap::new(),
        }
    }

    fn process_transaction(&mut self, transaction: Transaction) {
        if let Some(account) = self.accounts.get(&transaction.client) {
            if account.locked {
                return;
            }
        }

        match transaction.tx_type {
            TransactionType::Deposit => self.handle_deposit(transaction),
            TransactionType::Withdrawal => self.handle_withdrawal(transaction),
            TransactionType::Dispute => self.handle_dispute(transaction),
            TransactionType::Resolve => self.handle_resolve(transaction),
            TransactionType::Chargeback => self.handle_chargeback(transaction),
        }
    }

    fn handle_deposit(&mut self, transaction: Transaction) {
        let account = self.accounts.entry(transaction.client)
            .or_insert(Account::new(transaction.client));
        account.available += transaction.amount;
        account.total += transaction.amount;
        self.transactions.insert(transaction.tx, TransactionDetails {
            amount: transaction.amount,
            disputed: false,
        });
    }

    fn handle_withdrawal(&mut self, transaction: Transaction) {
        let account = self.accounts.entry(transaction.client)
            .or_insert(Account::new(transaction.client));
        if account.available >= transaction.amount {
            account.available -= transaction.amount;
            account.total -= transaction.amount;
            self.transactions.insert(transaction.tx, TransactionDetails {
                amount: transaction.amount,
                disputed: false,
            });
        }
    }

    fn handle_dispute(&mut self, transaction: Transaction) {
        let account = self.accounts.entry(transaction.client)
            .or_insert(Account::new(transaction.client));
        if let Some(transaction_details) = self.transactions.get_mut(&transaction.tx) {
            if !transaction_details.disputed && account.available >= transaction_details.amount {
                transaction_details.disputed = true;
                account.available -= transaction_details.amount;
                account.held += transaction_details.amount;
            }
        }
    }

    fn handle_resolve(&mut self, transaction: Transaction) {
        let account = self.accounts.entry(transaction.client)
            .or_insert(Account::new(transaction.client));
        if let Some(transaction_details) = self.transactions.get_mut(&transaction.tx) {
            if transaction_details.disputed && account.held >= transaction_details.amount {
                transaction_details.disputed = false;
                account.available += transaction_details.amount;
                account.held -= transaction_details.amount;
                self.transactions.remove(&transaction.tx);
            }
        }
    }

    fn handle_chargeback(&mut self, transaction: Transaction) {
        let account = self.accounts.entry(transaction.client)
            .or_insert(Account::new(transaction.client));
        if let Some(transaction_details) = self.transactions.get_mut(&transaction.tx) {
            if transaction_details.disputed && account.held >= transaction_details.amount {
                transaction_details.disputed = false;
                account.held -= transaction_details.amount;
                account.total -= transaction_details.amount;
                account.locked = true;
                self.transactions.remove(&transaction.tx);
            }
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        return Err("Usage: cargo run -- <input_file.csv>".into());
    }


    let input_file = File::open(&args[1])?;
    let mut reader = ReaderBuilder::new()
        .trim(csv::Trim::All)
        .flexible(true)
        .from_reader(BufReader::new(input_file));

    let mut engine = TransactionEngine::new();

    for result in reader.deserialize() {
        engine.process_transaction(result?);
    }

    let mut writer = WriterBuilder::new().from_writer(std::io::stdout());
    for (i, account) in engine.accounts.values().enumerate() {
        writer.serialize(account)?;
        if i % 100 == 0 {
            writer.flush()?;
        }
    }
    writer.flush()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_deserialize_transaction() {
        let input = r#"type,client,tx,amount
deposit,1,1,1.0
withdrawal,1,2,2.0
dispute,1,1,
resolve,1,1,
chargeback,1,1,
"#;

        let mut reader = ReaderBuilder::new()
            .trim(csv::Trim::All)
            .flexible(true)
            .from_reader(Cursor::new(input));

        let transactions: Vec<Transaction> = reader.deserialize().collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(transactions.len(), 5);
        assert_eq!(transactions[0].tx_type, TransactionType::Deposit);
        assert_eq!(transactions[0].client, 1);
        assert_eq!(transactions[0].tx, 1);
        assert_eq!(transactions[0].amount, Decimal::new(10, 1));

        assert_eq!(transactions[1].tx_type, TransactionType::Withdrawal);
        assert_eq!(transactions[1].client, 1);
        assert_eq!(transactions[1].tx, 2);
        assert_eq!(transactions[1].amount, Decimal::new(20, 1));

        assert_eq!(transactions[2].tx_type, TransactionType::Dispute);
        assert_eq!(transactions[2].client, 1);
        assert_eq!(transactions[2].tx, 1);
        assert_eq!(transactions[2].amount, Decimal::new(0, 1));

        assert_eq!(transactions[3].tx_type, TransactionType::Resolve);
        assert_eq!(transactions[3].client, 1);
        assert_eq!(transactions[3].tx, 1);
        assert_eq!(transactions[3].amount, Decimal::new(0, 1));

        assert_eq!(transactions[4].tx_type, TransactionType::Chargeback);
        assert_eq!(transactions[4].client, 1);
        assert_eq!(transactions[4].tx, 1);
        assert_eq!(transactions[4].amount, Decimal::new(0, 1));
    }

    #[test]
    fn test_serialize_account() {
        let account = Account {
            client: 1,
            available: Decimal::new(10, 0),
            held: Decimal::new(0, 1),
            total: Decimal::new(10, 0),
            locked: false,
        };

        let mut writer = WriterBuilder::new().from_writer(Vec::new());
        writer.serialize(account).unwrap();
        let output = writer.into_inner().unwrap();

        assert_eq!(String::from_utf8(output).unwrap(), "client,available,held,total,locked\n1,10,0,10,false\n");
    }

    #[test]
    fn test_flow() {
        let mut transaction_engine = TransactionEngine::new();

        // make some deposits
        transaction_engine.process_transaction(Transaction {
            tx_type: TransactionType::Deposit,
            client: 1,
            tx: 1,
            amount: Decimal::new(10, 0),
        });
        assert_eq!(transaction_engine.accounts.get(&1).unwrap().available, Decimal::new(10, 0));
        assert_eq!(transaction_engine.accounts.get(&1).unwrap().held, Decimal::new(0, 1));
        assert_eq!(transaction_engine.accounts.get(&1).unwrap().total, Decimal::new(10, 0));
        assert_eq!(transaction_engine.transactions.get(&1).unwrap().amount, Decimal::new(10, 0));
        transaction_engine.process_transaction(Transaction {
            tx_type: TransactionType::Deposit,
            client: 2,
            tx: 2,
            amount: Decimal::new(20, 0),
        });
        assert_eq!(transaction_engine.accounts.get(&2).unwrap().available, Decimal::new(20, 0));
        assert_eq!(transaction_engine.accounts.get(&2).unwrap().held, Decimal::new(0, 1));
        assert_eq!(transaction_engine.accounts.get(&2).unwrap().total, Decimal::new(20, 0));
        assert_eq!(transaction_engine.transactions.get(&2).unwrap().amount, Decimal::new(20, 0));

        // withdraw successfully
        transaction_engine.process_transaction(Transaction {
            tx_type: TransactionType::Withdrawal,
            client: 1,
            tx: 3,
            amount: Decimal::new(1, 0),
        });
        assert_eq!(transaction_engine.accounts.get(&1).unwrap().available, Decimal::new(9, 0));
        assert_eq!(transaction_engine.accounts.get(&1).unwrap().held, Decimal::new(0, 1));
        assert_eq!(transaction_engine.accounts.get(&1).unwrap().total, Decimal::new(9, 0));
        assert_eq!(transaction_engine.transactions.get(&3).unwrap().amount, Decimal::new(1, 0));

        // withdraw unsuccessfully
        transaction_engine.process_transaction(Transaction {
            tx_type: TransactionType::Withdrawal,
            client: 1,
            tx: 4,
            amount: Decimal::new(100, 0),
        });
        // since the account has only 10 available, the withdrawal should not be processed
        assert_eq!(transaction_engine.accounts.get(&1).unwrap().available, Decimal::new(9, 0));
        assert_eq!(transaction_engine.accounts.get(&1).unwrap().held, Decimal::new(0, 1));
        assert_eq!(transaction_engine.accounts.get(&1).unwrap().total, Decimal::new(9, 0));
        assert!(transaction_engine.transactions.get(&4).is_none());

        // dispute successfully
        transaction_engine.process_transaction(Transaction {
            tx_type: TransactionType::Dispute,
            client: 1,
            tx: 3,
            amount: Decimal::new(0, 1),
        });
        assert!(transaction_engine.transactions.get(&3).unwrap().disputed);
        assert_eq!(transaction_engine.accounts.get(&1).unwrap().available, Decimal::new(8, 0));
        assert_eq!(transaction_engine.accounts.get(&1).unwrap().held, Decimal::new(1, 0));
        assert_eq!(transaction_engine.accounts.get(&1).unwrap().total, Decimal::new(9, 0));

        // dispute already disputed transaction
        transaction_engine.process_transaction(Transaction {
            tx_type: TransactionType::Dispute,
            client: 1,
            tx: 3,
            amount: Decimal::new(0, 1),
        });
        // nothing changes
        assert!(transaction_engine.transactions.get(&3).unwrap().disputed);
        assert_eq!(transaction_engine.accounts.get(&1).unwrap().available, Decimal::new(8, 0));
        assert_eq!(transaction_engine.accounts.get(&1).unwrap().held, Decimal::new(1, 0));
        assert_eq!(transaction_engine.accounts.get(&1).unwrap().total, Decimal::new(9, 0));

        // dispute non-existent transaction
        transaction_engine.process_transaction(Transaction {
            tx_type: TransactionType::Dispute,
            client: 1,
            tx: 5,
            amount: Decimal::new(0, 1),
        });
        // nothing changes
        assert!(transaction_engine.transactions.get(&6).is_none());
        assert_eq!(transaction_engine.accounts.get(&1).unwrap().available, Decimal::new(8, 0));
        assert_eq!(transaction_engine.accounts.get(&1).unwrap().held, Decimal::new(1, 0));
        assert_eq!(transaction_engine.accounts.get(&1).unwrap().total, Decimal::new(9, 0));


        // resolve successfully
        transaction_engine.process_transaction(Transaction {
            tx_type: TransactionType::Resolve,
            client: 1,
            tx: 3,
            amount: Decimal::new(0, 1),
        });
        assert!(transaction_engine.transactions.get(&3).is_none());
        assert_eq!(transaction_engine.accounts.get(&1).unwrap().available, Decimal::new(9, 0));
        assert_eq!(transaction_engine.accounts.get(&1).unwrap().held, Decimal::new(0, 1));
        assert_eq!(transaction_engine.accounts.get(&1).unwrap().total, Decimal::new(9, 0));

        // resolve unsuccessfully, un-disputed transaction
        transaction_engine.process_transaction(Transaction {
            tx_type: TransactionType::Resolve,
            client: 2,
            tx: 2,
            amount: Decimal::new(0, 1),
        });
        // nothing changes
        assert!(!transaction_engine.transactions.get(&2).unwrap().disputed);
        assert_eq!(transaction_engine.accounts.get(&2).unwrap().available, Decimal::new(20, 0));
        assert_eq!(transaction_engine.accounts.get(&2).unwrap().held, Decimal::new(0, 1));
        assert_eq!(transaction_engine.accounts.get(&2).unwrap().total, Decimal::new(20, 0));

        // chargeback successfully
        transaction_engine.process_transaction(Transaction {
            tx_type: TransactionType::Withdrawal,
            client: 2,
            tx: 4,
            amount: Decimal::new(5, 0),
        });
        transaction_engine.process_transaction(Transaction {
            tx_type: TransactionType::Dispute,
            client: 2,
            tx: 4,
            amount: Decimal::new(0, 1),
        });
        transaction_engine.process_transaction(Transaction {
            tx_type: TransactionType::Chargeback,
            client: 2,
            tx: 4,
            amount: Decimal::new(0, 1),
        });
        assert!(transaction_engine.transactions.get(&4).is_none());
        assert_eq!(transaction_engine.accounts.get(&2).unwrap().available, Decimal::new(10, 0));
        assert_eq!(transaction_engine.accounts.get(&2).unwrap().held, Decimal::new(0, 1));
        assert_eq!(transaction_engine.accounts.get(&2).unwrap().total, Decimal::new(10, 0));
        assert!(transaction_engine.accounts.get(&2).unwrap().locked);

        // chargeback unsuccessfully, non-existent transaction
        transaction_engine.process_transaction(Transaction {
            tx_type: TransactionType::Chargeback,
            client: 1,
            tx: 6,
            amount: Decimal::new(0, 1),
        });
        // nothing changes
        assert_eq!(transaction_engine.accounts.get(&1).unwrap().available, Decimal::new(9, 0));
        assert_eq!(transaction_engine.accounts.get(&1).unwrap().held, Decimal::new(0, 1));
        assert_eq!(transaction_engine.accounts.get(&1).unwrap().total, Decimal::new(9, 0));

        // chargeback unsuccessfully, non-disputed transaction
        transaction_engine.process_transaction(Transaction {
            tx_type: TransactionType::Chargeback,
            client: 1,
            tx: 1,
            amount: Decimal::new(0, 1),
        });
        // nothing changes
        assert_eq!(transaction_engine.accounts.get(&1).unwrap().available, Decimal::new(9, 0));
        assert_eq!(transaction_engine.accounts.get(&1).unwrap().held, Decimal::new(0, 1));
        assert_eq!(transaction_engine.accounts.get(&1).unwrap().total, Decimal::new(9, 0));

        // ignored transaction on locked account
        transaction_engine.process_transaction(Transaction {
            tx_type: TransactionType::Deposit,
            client: 2,
            tx: 5,
            amount: Decimal::new(10, 0),
        });
        // nothing changes
        assert_eq!(transaction_engine.accounts.get(&2).unwrap().available, Decimal::new(10, 0));
        assert_eq!(transaction_engine.accounts.get(&2).unwrap().held, Decimal::new(0, 1));
        assert_eq!(transaction_engine.accounts.get(&2).unwrap().total, Decimal::new(10, 0));
        // transaction does not exist
        assert!(transaction_engine.transactions.get(&5).is_none());
    }
}

