# Transaction Schema

## Description

Streaming banking transactions.

## Frequency

Every 2 Minutes

## Processing

Streaming

## Primary Key

transaction_id

## Foreign Key

account_id

## Columns

| Column | Data Type |
|----------|-----------|
| transaction_id | STRING |
| account_id | STRING |
| transaction_timestamp | TIMESTAMP |
| transaction_type | STRING |
| amount | DECIMAL(18,2) |
| currency | STRING |
| merchant_id | STRING |
| channel | STRING |
| branch_id | STRING |
| transaction_status | STRING |