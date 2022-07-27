from Constants import RESOURCE_STATUS

class Lock:
    def __init__(self, resource):
        self.resource = resource
        self.lock_list = [] # list of transactions having lock on the resource
        self.wait_list = [] # list of transactions waiting for the resource
        self.resource_state = None