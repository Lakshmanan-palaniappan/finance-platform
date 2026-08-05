| Entity          | Primary Key        | Parent   | Category  | Processing |
| --------------- | ------------------ | -------- | --------- | ---------- |
| Customer        | customer_id        | -        | Master    | CDC + SCD2 |
| Account         | account_id         | Customer | Master    | CDC + SCD2 |
| Loan            | loan_id            | Customer | Master    | CDC + SCD2 |
| Card            | card_id            | Customer | Master    | CDC + SCD2 |
| Branch          | branch_id          | -        | Reference | Batch      |
| Transaction     | transaction_id     | Account  | Event     | Streaming  |
| ATM Transaction | atm_transaction_id | Account  | Event     | Streaming  |
| Login Activity  | login_id           | Customer | Event     | Streaming  |
| KYC             | customer_id        | Customer | Master    | Batch      |
| Exchange Rate   | currency_code      | -        | Reference | Batch      |
