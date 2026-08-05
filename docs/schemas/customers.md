# Customer Schema

## Description

Stores master information for bank customers.

## Source System

Customer Management System

## Frequency

Daily

## Processing

Batch
CDC
SCD Type 2

---

## Primary Key

customer_id

---

## Columns

| Column | Data Type | Nullable | Validation |
|----------|-----------|----------|------------|
| customer_id | STRING | No | Unique |
| first_name | STRING | No | Required |
| last_name | STRING | No | Required |
| date_of_birth | DATE | No | Age >= 18 |
| gender | STRING | Yes | M/F/O |
| email | STRING | Yes | Valid email |
| phone | STRING | No | 10 digits |
| pan | STRING | No | Unique |
| aadhaar | STRING | No | 12 digits |
| occupation | STRING | Yes | Optional |
| annual_income | DECIMAL(15,2) | Yes | >=0 |
| branch_id | STRING | No | Must exist |
| customer_status | STRING | No | ACTIVE / INACTIVE |
| created_timestamp | TIMESTAMP | No | System Generated |
| updated_timestamp | TIMESTAMP | No | System Generated |