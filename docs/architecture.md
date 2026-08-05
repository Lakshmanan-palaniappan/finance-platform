# Architecture Design

## Objective

Design and implement an enterprise banking analytics platform capable of processing both batch and streaming banking data.

---

## Business Problem

The bank has multiple operational systems.

Each system exports data independently.

The organization needs a centralized analytics platform capable of

- Customer Analytics
- Transaction Analytics
- Branch Analytics
- Loan Analytics
- Fraud Detection

---

## Source Systems

| System | Frequency |
|----------|-----------|
| Customer Management | Daily |
| Account Management | Daily |
| Loan System | Daily |
| Card System | Daily |
| Branch Management | Weekly |
| Exchange Rates | Daily |
| Transaction Processing | Every 5 minutes |
| ATM Network | Every 5 minutes |
| Internet Banking | Every minute |

---

## High Level Architecture

Python Banking Simulator

↓

Azure Data Lake Storage

↓

Auto Loader

↓

Lakeflow Declarative Pipelines

↓

Bronze

↓

Silver

↓

Gold

↓

Dashboards