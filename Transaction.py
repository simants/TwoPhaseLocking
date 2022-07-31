from Constants import TRANSACTION_STATUS

class Transaction:
    def __init__(self, transaction_id, time_stamp):
        self.transaction_id = transaction_id
        self.time_stamp = time_stamp
        self.resource_hold = [] # list of resources / items locked by a transaction
        self.transaction_state = TRANSACTION_STATUS.get('ACTIVE')
        self.waiting = [] # list of waiting operations when transaction is in BLOCKED state
    
    def __str__(self) -> str:
        return f"Id={self.transaction_id}. TS={self.time_stamp}. state={self.transaction_state}"

    def reset(self, transaction_state):
        self.resource_hold = []
        self.waiting = []
        self.transaction_state = TRANSACTION_STATUS.get(transaction_state)