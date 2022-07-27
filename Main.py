import sys
import re

from Constants import TRANSACTION_STATUS, RESOURCE_STATUS
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

    def __init__(self) -> None:
        self.TRANSACTION_TABLE = {}
        self.LOCK_TABLE = {}
        self.transaction_timestamp = 1
        self.operation = None  # Current operation in action
        self.transaction_id = None  # Current transaction in action
        self.resource = None  # Current resource / item in action

    # Function to begin a new transaction
    def begin(self):

        if self.transaction_id not in self.TRANSACTION_TABLE:
            self.TRANSACTION_TABLE[self.transaction_id] = Transaction(self.transaction_id, self.transaction_timestamp)
            self.transaction_timestamp += 1

    def start_execution(self, file_input_list):

        for input in file_input_list:
            self.operation = input[0]
            self.transaction_id = input[1]
            self.resource = input[2] if len(input) > 2 else None

            self.execute()

    def execute(self, operation=None):
        if self.operation == 'b':
            self.begin()

        elif self.operation == 'r':
            self.read_lock()

        elif self.operation == 'w':
            self.write_lock()

        elif self.operation == 'e':
            self.commit()

    def read_lock(self):
        pass

    def write_lock(self):
        pass

    def commmit(self):
        pass

    def unlock_resource(self, resource_list):
        pass

    def wait_die(self, requesting_id, holding_id, resource):
        pass

    def write_to_file(self, input):
        output_file = open(sys.argv[2], 'w')
        output_file.write(input)
        output_file.close()

if len(sys.argv) < 3:
    print('Input given is wrong.\n Expected input: file_name <input.txt> <output.txt>')


file_input = []

with open(sys.argv[1],'rt') as input:
    lines = input.readlines()
    for line in lines:
        file_input.append(parse_input(line))

Main().start_execution(file_input)