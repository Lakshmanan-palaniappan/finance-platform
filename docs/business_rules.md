# Business Rules

## Customer

- Customer ID cannot be null.
- PAN must be unique.
- Aadhaar must be unique.
- Email must be valid.

---

## Account

- Customer must exist.
- Balance cannot be negative.
- Account status must be valid.

---

## Transactions

- Amount must be greater than zero.
- Account must exist.
- Transaction timestamp is mandatory.
- Currency must be supported.

---

## Fraud Rules

### Rule 1

Withdrawal greater than ₹200000

Action

Generate Fraud Alert

---

### Rule 2

More than five failed logins

Action

Block Account

---

### Rule 3

Impossible travel

Action

Generate Fraud Alert

---

### Rule 4

Twenty transactions within one minute

Action

Generate Fraud Alert