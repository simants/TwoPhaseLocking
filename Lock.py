from Constants import RESOURCE_STATUS

class Lock:
    def __init__(self, resource, state=None):
        self.resource = resource
        self.lock_list = [] # list of transactions having lock on the resource
        self.wait_list = [] # list of transactions waiting for the resource
        self.resource_state = state

    def __str__(self):
        return f'{self.resource}: locked by: {self.lock_list}, waiting: {self.wait_list}, state={self.resource_state}'