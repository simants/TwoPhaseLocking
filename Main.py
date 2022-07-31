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
        return [line[0],
                int(re.findall("\d+", line)[0]),
                re.findall('[A-Z a-z]?', line.split("(")[1])[0]]


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

            self.write_to_file(
                f'{self.get_prefix()} T{self.transaction_id} begins. {self.TRANSACTION_TABLE[self.transaction_id]}.\n')

    def start_execution(self, file_input_list):

        for input in file_input_list:
            self.operation = input[0]
            self.transaction_id = input[1]
            self.resource = input[2] if len(input) > 2 else None
            self.execute()

    def execute(self, operation=None):

        if self.operation == OPERATIONS.get('BEGIN'):
            self.begin()

        elif self.operation == OPERATIONS.get('END'):
            self.commit()

        else:
            # Check for aborted / completed transactions
            if self.TRANSACTION_TABLE[self.transaction_id].transaction_state in ['committed', 'aborted']:
                pass
            elif self.operation == OPERATIONS.get('READ'):
                self.read_lock(operation)

            elif self.operation == OPERATIONS.get('WRITE'):
                self.write_lock()

        for key in self.LOCK_TABLE:
            print(f'LOCK_TABLE:- {key} : {self.LOCK_TABLE[key]}')

        for key in self.TRANSACTION_TABLE:
            print(f'TRANSACTION_TABLE:- {key} : {self.TRANSACTION_TABLE[key]}')

    def read_lock(self, operation=None):

        # Check if resource is already present in LOCK_TABLE
        if self.resource in self.LOCK_TABLE:

            if self.LOCK_TABLE[self.resource].resource_state == RESOURCE_STATUS.get('WRITE_LOCKED'):
                # Conflict due to write lock on resource, calling wait-die to resolve
                self.wait_die(self.transaction_id, self.LOCK_TABLE[self.resource].lock_list[0], 'WRITE_LOCKED')
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
                if operation and operation in self.TRANSACTION_TABLE[self.transaction_id].waiting:
                    self.TRANSACTION_TABLE[self.transaction_id].waiting.remove(operation)
        else:
            # Transation issues read lock
            self.write_to_file(f'{self.get_prefix()} {self.resource} is read locked by T{self.transaction_id}.\n')
            if self.resource not in self.TRANSACTION_TABLE[self.transaction_id].resource_hold:
                self.TRANSACTION_TABLE[self.transaction_id].resource_hold.append(self.resource)
            self.LOCK_TABLE[self.resource] = Lock(self.resource, RESOURCE_STATUS.get('READ_LOCKED'))
            self.LOCK_TABLE[self.resource].lock_list.append(self.transaction_id)

    def write_lock(self, operation=None):
        # Check if resource is already present in LOCK_TABLE
        if self.resource in self.LOCK_TABLE:

            # Check for lock state (Read or Write)
            if self.LOCK_TABLE[self.resource].resource_state == RESOURCE_STATUS.get('READ_LOCKED'):
                # Transaction upgrades to write lock 
                # if it is the only transaction holding read lock
                if len(self.LOCK_TABLE[self.resource].lock_list) == 1:
                    if self.LOCK_TABLE[self.resource].lock_list[0] == self.transaction_id:
                        self.write_to_file(
                            f'{self.get_prefix()} read lock on {self.resource} by T{self.transaction_id} is upgraded to write lock.\n')
                        self.LOCK_TABLE[self.resource].resource_state = RESOURCE_STATUS.get('WRITE_LOCKED')
                else:
                    # Multiple transactions holding read lock, call wait-die to resolve conflict
                    self.wait_die(self.transaction_id, self.LOCK_TABLE[self.resource].lock_list[0], 'READ_LOCKED')

            elif self.LOCK_TABLE[self.resource].resource_state == RESOURCE_STATUS.get('WRITE_LOCKED'):
                pass

    def commmit(self, transaction_id):

        self.write_to_file(f'{self.get_prefix()} T{self.transaction_id} is committed.\n')

        resource_release = deepcopy(self.TRANSACTION_TABLE[transaction_id].resource_hold)
        for resource in resource_release:
            self.TRANSACTION_TABLE[transaction_id].resource_hold.remove(resource)
            if transaction_id in self.LOCK_TABLE[resource].lock_list:
                self.unlock_resource(resource)

        for resource in resource_release:
            if self.LOCK_TABLE[resource].wait_list:
                transaction_waiting = self.LOCK_TABLE[resource].wait_list.pop(0)
                resume_message = f'T{transaction_waiting} resumed operation from wait-list for resource {resource}'
                print(resume_message)
                waiting_operations = deepcopy(self.TRANSACTION_TABLE[transaction_waiting].waiting)
                for operation in waiting_operations:
                    self.TRANSACTION_TABLE[transaction_waiting].waiting.remove(operation)
                    self.operation = operation[0]
                    self.transaction_id = operation[1]
                    self.resource = operation[3]
                    self.execute()

        del self.TRANSACTION_TABLE[transaction_id]

    def unlock_resource(self, resource):
        self.LOCK_TABLE[resource].lock_list.remove(self.transaction_id)
        if len(self.LOCK_TABLE[resource].lock_list) > 0:
            self.LOCK_TABLE[resource].resource_state = self.RESOURCE_STATUS.get('READ_LOCKED')
        else:
            del self.LOCK_TABLE[resource]

    def abort(self, transaction_id):
        
        self.write_to_file(f'{self.get_prefix()} T{transaction_id} aborted due to wait-die')

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

                    self.execute()
        
        del self.TRANSACTION_TABLE[transaction_id]

    def wait_die(self, requesting_id, holding_id, resource_state):

        # Elder transaction is blocked if waiting for resource locked by younger transaction
        # Operation is added to transaction's waiting list
        # Transaction Id added to Lock table under resource;s waiting list
        if self.TRANSACTION_TABLE[requesting_id].time_stamp < self.TRANSACTION_TABLE[holding_id].time_stamp:
            self.write_to_file(f'{self.get_prefix()} T{requesting_id} is blocked/waiting due to wait-die.\n')
            self.TRANSACTION_TABLE[requesting_id].waiting.append((self.operation, self.transaction_id, self.resource))
            self.TRANSACTION_TABLE[requesting_id].transaction_state = TRANSACTION_STATUS.get('BLOCKED')
            if requesting_id not in self.LOCK_TABLE[self.resource].wait_list:
                self.LOCK_TABLE[self.resource].wait_list.append(requesting_id)
        # Younger transaction dies if requesting resource locked by elder transaction
        else:
            self.write_to_file(f'{self.get_prefix()} T{requesting_id} is aborted due to wait-die.\n')
            # Abort younger transaction and release locks 
            self.abort()
            if requesting_id not in self.LOCK_TABLE[self.resource].lock_list:
                self.LOCK_TABLE[self.resource].lock_list.append(requesting_id)
                self.LOCK_TABLE[self.resource].resource_state = RESOURCE_STATUS.get(resource_state)

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

Main().start_execution(file_input)
