# Loan Schema

## Processing

CDC
SCD Type 2

## Columns

| Column | Data Type |
|----------|-----------|
| loan_id | STRING |
| customer_id | STRING |
| loan_type | STRING |
| principal_amount | DECIMAL(18,2) |
| interest_rate | DECIMAL(5,2) |
| emi | DECIMAL(18,2) |
| outstanding_amount | DECIMAL(18,2) |
| loan_status | STRING |
| start_date | DATE |
| end_date | DATE |