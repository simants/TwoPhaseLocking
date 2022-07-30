from Constants import TRANSACTION_STATUS

class Transaction:
    def __init__(self, transaction_id, time_stamp):
        self.transaction_id = transaction_id
        self.time_stamp = time_stamp
        self.resource_hold = [] # list of resources / items locked by a transaction
        self.transaction_state = TRANSACTION_STATUS['ACTIVE']
        self.waiting = [] # list of waiting operations when transaction is in BLOCKED state
    
    def hold_resource(self, resource):
        self.resource_hold.append(resource)
    
    def add_waiting_operation(self, operation):
        self.waiting.append(operation)
    
    def __str__(self) -> str:
        return f"{self.transaction_id} : [{self.time_stamp}, {self.transaction_state}]"