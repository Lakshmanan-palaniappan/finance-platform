from finance_bundle.common.paths import ROOT


class Checkpoints:

    ROOT = f"{ROOT}/checkpoints"
    
class BronzeCheckpoint:

    CUSTOMER = f"{Checkpoints.ROOT}/bronze/customer"

    ACCOUNT = f"{Checkpoints.ROOT}/bronze/account"

    LOAN = f"{Checkpoints.ROOT}/bronze/loan"

    CARD = f"{Checkpoints.ROOT}/bronze/card"

    TRANSACTION = f"{Checkpoints.ROOT}/bronze/transaction"
    
class SilverCheckpoint:

    CUSTOMER = f"{Checkpoints.ROOT}/silver/customer"

    ACCOUNT = f"{Checkpoints.ROOT}/silver/account"

    LOAN = f"{Checkpoints.ROOT}/silver/loan"