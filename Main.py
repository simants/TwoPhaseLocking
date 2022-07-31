from copy import deepcopy
import sys
import re
from copy import deepcopy

from Constants import TRANSACTION_STATUS, RESOURCE_STATUS, OPERATIONS
from Transaction import Transaction
from Lock import Lock


def parse_input(line):
    if re.match("b|e\d", line):
        return [line[0], int(re.findall("\d", line)[0])]

    elif re.match('r|w\d\s*\([A-Z a-z]?\)', line):
        return [line[0],int(re.findall("\d+", line)[0]),re.findall('[A-Z a-z]?', line.split("(")[1])[0]]


class Main:

    def __init__(self):
        self.TRANSACTION_TABLE = {}
        self.LOCK_TABLE = {}
        self.transaction_timestamp = 1
        self.operation = None  # Current operation in action
        self.transaction_id = None  # Current transaction in action
        self.resource = None  # Current resource / item in action

    # Function to get message prefix
    def get_prefix(self):

        if self.resource != None:
            return f'{self.operation}{self.transaction_id}({self.resource});'
        else:
            return f'{self.operation}{self.transaction_id};'

    # Function to begin a new transaction
    def begin(self):

        if self.transaction_id not in self.TRANSACTION_TABLE:
            self.TRANSACTION_TABLE[self.transaction_id] = Transaction(self.transaction_id, self.transaction_timestamp)
            self.transaction_timestamp += 1

            self.write_to_file(f'{self.get_prefix()} T{self.transaction_id} begins. {self.TRANSACTION_TABLE[self.transaction_id]}.\n')

    def start_execution(self, file_input_list):

        for input in file_input_list:
            self.operation = input[0]
            self.transaction_id = input[1]
            self.resource = input[2] if len(input) > 2 else None
            self.execute()

    def execute(self, resume=False):

        if self.operation == OPERATIONS.get('BEGIN'):
            self.begin()
        
        elif self.TRANSACTION_TABLE[self.transaction_id].transaction_state == TRANSACTION_STATUS['ABORTED']:
            self.write_to_file(f'{self.get_prefix()} T{self.transaction_id} is already aborted.\n')

        elif self.operation == OPERATIONS.get('READ'):
            self.read_lock(resume)

        elif self.operation == OPERATIONS.get('WRITE'):
            self.write_lock(resume)

        elif self.operation == OPERATIONS.get('END'):
            self.commit(self.transaction_id, resume)

    def read_lock(self, resume=False):

        # Check if transaction is resumed or not
        if not resume and self.TRANSACTION_TABLE[self.transaction_id].transaction_state == TRANSACTION_STATUS['BLOCKED']:
            self.TRANSACTION_TABLE[self.transaction_id].waiting.append((self.operation, self.transaction_id, self.resource))
            self.write_to_file(f'{self.get_prefix()} {self.get_prefix()} for T{self.transaction_id} is added to waiting list.\n')
                    
        # Check if resource is already present in LOCK_TABLE
        elif self.resource in self.LOCK_TABLE:

            if self.LOCK_TABLE[self.resource].resource_state == RESOURCE_STATUS.get('WRITE_LOCKED'):
                # Conflict due to write lock on resource, calling wait-die to resolve
                self.wait_die(self.transaction_id, self.LOCK_TABLE[self.resource].lock_list[-1], 'WRITE_LOCKED')
            else:
                # Transation issues read lock
                self.write_to_file(f'{self.get_prefix()} {self.resource} is read locked by T{self.transaction_id}.\n')

                # Check if transaction is active or resumed
                if self.TRANSACTION_TABLE[self.transaction_id].transaction_state == TRANSACTION_STATUS.get('ACTIVE'):
                    if self.resource not in self.TRANSACTION_TABLE[self.transaction_id].resource_hold:
                        self.TRANSACTION_TABLE[self.transaction_id].resource_hold.append(self.resource)
                elif self.TRANSACTION_TABLE[self.transaction_id].transaction_state == TRANSACTION_STATUS.get('BLOCKED'):
                    self.TRANSACTION_TABLE[self.transaction_id].transaction_state = TRANSACTION_STATUS.get('ACTIVE')

                # Add transaction id to resource's lock_list
                if self.transaction_id not in self.LOCK_TABLE[self.resource].lock_list:
                    self.LOCK_TABLE[self.resource].lock_list.append(self.transaction_id)
                    self.LOCK_TABLE[self.resource].resource_state = RESOURCE_STATUS.get('READ_LOCKED')
        else:
            # Transation issues read lock
            self.write_to_file(f'{self.get_prefix()} {self.resource} is read locked by T{self.transaction_id}.\n')
            if self.resource not in self.TRANSACTION_TABLE[self.transaction_id].resource_hold:
                self.TRANSACTION_TABLE[self.transaction_id].resource_hold.append(self.resource)
            self.LOCK_TABLE[self.resource] = Lock(self.resource, RESOURCE_STATUS.get('READ_LOCKED'))
            self.LOCK_TABLE[self.resource].lock_list.append(self.transaction_id)

    def write_lock(self, resume=False):

        # Check if transaction is resumed or not
        if not resume and self.TRANSACTION_TABLE[self.transaction_id].transaction_state == TRANSACTION_STATUS['BLOCKED']:
            self.TRANSACTION_TABLE[self.transaction_id].waiting.append((self.operation, self.transaction_id, self.resource))
            self.write_to_file(f'{self.get_prefix()} {self.get_prefix()} for T{self.transaction_id} is added to waiting list.\n')

        # Check if resource is already present in LOCK_TABLE
        elif self.resource in self.LOCK_TABLE:

            # Check for lock state (Read or Write)
            if self.LOCK_TABLE[self.resource].resource_state == RESOURCE_STATUS.get('READ_LOCKED'):
                # Transaction upgrades to write lock 
                # if it is the only transaction holding read lock
                if len(self.LOCK_TABLE[self.resource].lock_list) == 1:
                    if self.LOCK_TABLE[self.resource].lock_list[0] == self.transaction_id:
                        self.write_to_file(
                            f'{self.get_prefix()} read lock on {self.resource} by T{self.transaction_id} is upgraded to write lock.\n')
                        self.LOCK_TABLE[self.resource].resource_state = RESOURCE_STATUS.get('WRITE_LOCKED')

                        # Check if transaction is active or resumed
                        if self.TRANSACTION_TABLE[self.transaction_id].transaction_state == TRANSACTION_STATUS.get('ACTIVE'):
                            if self.resource not in self.TRANSACTION_TABLE[self.transaction_id].resource_hold:
                                self.TRANSACTION_TABLE[self.transaction_id].resource_hold.append(self.resource)
                        elif self.TRANSACTION_TABLE[self.transaction_id].transaction_state == TRANSACTION_STATUS.get('BLOCKED'):
                            self.TRANSACTION_TABLE[self.transaction_id].transaction_state = TRANSACTION_STATUS.get('ACTIVE')
                else:
                    # Multiple transactions holding read lock, call wait-die to resolve conflict
                    self.wait_die(self.transaction_id, self.LOCK_TABLE[self.resource].lock_list[-1], 'READ_LOCKED')

            elif self.LOCK_TABLE[self.resource].resource_state == RESOURCE_STATUS.get('WRITE_LOCKED'):
                #Conflict due to write lock, call wait-die to resolve
                self.wait_die(self.transaction_id, self.LOCK_TABLE[self.resource].lock_list[-1], 'WRITE_LOCKED')

    def commit(self, transaction_id, resume=False):

        # Check if transaction is resumed or not
        if not resume and self.TRANSACTION_TABLE[self.transaction_id].transaction_state == TRANSACTION_STATUS['BLOCKED']:
            self.TRANSACTION_TABLE[self.transaction_id].waiting.append((self.operation, self.transaction_id, self.resource))
            self.write_to_file(f'{self.get_prefix()} Comitting T{self.transaction_id} is added to waiting list.\n')
            return None

        self.write_to_file(f'{self.get_prefix()} T{self.transaction_id} is committed.\n')
        
        resource_release = deepcopy(self.TRANSACTION_TABLE[transaction_id].resource_hold)
        for resource in resource_release:
            self.TRANSACTION_TABLE[transaction_id].resource_hold.remove(resource)
            if transaction_id in self.LOCK_TABLE[resource].lock_list:
                self.unlock_resource(resource)

        for resource in resource_release:
            if self.LOCK_TABLE[resource].wait_list:
                transaction_waiting = self.LOCK_TABLE[resource].wait_list.pop(0)
                waiting_operations = deepcopy(self.TRANSACTION_TABLE[transaction_waiting].waiting)
                for operation in waiting_operations:
                    self.TRANSACTION_TABLE[transaction_waiting].waiting.remove(operation)
                    self.operation = operation[0]
                    self.transaction_id = operation[1]
                    self.resource = operation[2]
                    self.execute(resume=True)

        self.TRANSACTION_TABLE[transaction_id].reset('COMMITTED')

    def unlock_resource(self, resource):
        self.LOCK_TABLE[resource].lock_list.remove(self.transaction_id)
        if len(self.LOCK_TABLE[resource].lock_list) > 0:
            self.LOCK_TABLE[resource].resource_state = RESOURCE_STATUS.get('READ_LOCKED')
        else:
            self.LOCK_TABLE[resource].resource_state = None

    def abort(self, transaction_id):
        
        self.write_to_file(f'{self.get_prefix()} T{transaction_id} aborted due to wait-die.\n')

        resource_list = deepcopy(self.TRANSACTION_TABLE[transaction_id].resource_hold)

        # Unlock resources locked by the transaction
        for resource in resource_list:
            self.TRANSACTION_TABLE[transaction_id].resource_hold.remove(resource)
            if transaction_id in self.LOCK_TABLE[resource].lock_list:
                self.unlock_resource(resource)

            # Resume BLOCKED transactions
            if self.LOCK_TABLE[resource].wait_list:
                waiting_id = self.LOCK_TABLE[resource].wait_list.pop(0)
                waiting_operations = deepcopy(self.TRANSACTION_TABLE[waiting_id].waiting)
                
                for operation in waiting_operations:
                    self.TRANSACTION_TABLE[waiting_id].waiting.remove(operation)
                    self.operation = operation[0]
                    self.transaction_id = operation[1]
                    self.resource = operation[2]

                    self.execute(resume=True)
        
        self.TRANSACTION_TABLE[transaction_id].reset('ABORTED')

    def wait_die(self, requesting_id, holding_id, resource_state):

        # Elder transaction is blocked if waiting for resource locked by younger transaction
        # Operation is added to transaction's waiting list
        # Transaction Id added to Lock table under resource;s waiting list
        if self.TRANSACTION_TABLE[requesting_id].time_stamp < self.TRANSACTION_TABLE[holding_id].time_stamp:
            self.write_to_file(f'{self.get_prefix()} T{requesting_id} is blocked/waiting due to wait-die.\n')
            self.TRANSACTION_TABLE[requesting_id].waiting.append((self.operation, self.transaction_id, self.resource))
            self.TRANSACTION_TABLE[requesting_id].transaction_state = TRANSACTION_STATUS.get('BLOCKED')
            if self.resource and requesting_id not in self.LOCK_TABLE[self.resource].wait_list:
                self.LOCK_TABLE[self.resource].wait_list.append(requesting_id)
        # Younger transaction dies if requesting resource locked by elder transaction
        else:
            
            if self.resource and requesting_id not in self.LOCK_TABLE[self.resource].lock_list:
                self.LOCK_TABLE[self.resource].lock_list.append(requesting_id)
                self.LOCK_TABLE[self.resource].resource_state = RESOURCE_STATUS.get(resource_state)
            
            # Abort younger transaction and release locks 
            self.abort(self.transaction_id)

    def write_to_file(self, input):
        output_file = open(sys.argv[2], 'a')
        output_file.write(input)
        output_file.close()


if len(sys.argv) < 3:
    print('Input given is wrong.\n Expected input: file_name <input.txt> <output.txt>')

file_input = []

with open(sys.argv[1], 'rt') as input:
    lines = input.readlines()
    for line in lines:
        file_input.append(parse_input(line))
input.close()

Main().start_execution(file_input)
