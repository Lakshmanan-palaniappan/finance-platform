# Account Schema

## Description

Customer bank accounts.

## Frequency

Daily

## Processing

CDC
SCD Type 2

## Primary Key

account_id

## Foreign Key

customer_id

## Columns

| Column | Data Type |
|----------|-----------|
| account_id | STRING |
| customer_id | STRING |
| account_number | STRING |
| account_type | STRING |
| currency | STRING |
| balance | DECIMAL(18,2) |
| interest_rate | DECIMAL(5,2) |
| branch_id | STRING |
| account_status | STRING |
| opened_date | DATE |
| closed_date | DATE |
| created_timestamp | TIMESTAMP |
| updated_timestamp | TIMESTAMP |