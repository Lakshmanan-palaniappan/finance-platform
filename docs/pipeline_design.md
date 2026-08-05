# Pipeline Design

Landing

↓

Auto Loader

↓

Bronze

↓

Silver

↓

Gold

---

## Bronze

Responsibilities

- Read CSV
- Preserve raw data
- Add ingestion metadata
- Store Delta

---

## Silver

Responsibilities

- Clean data
- Validate
- Remove duplicates
- Apply Expectations
- CDC
- SCD Type 2
- Quarantine invalid records

---

## Gold

Responsibilities

- Customer 360
- Branch Performance
- Daily Transactions
- Fraud Summary
- ATM Summary
- Executive Dashboard