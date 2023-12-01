from typing import List
from occ.data import Data

class Transaction:
    def __init__(self, name):
        self.name = name
        self.operations = []
        self.conflict = False
        self.data = {}
        self.read_set = set()
        self.write_set = set()

    def get_operation(self):
        return self.operations

    def read(self, data: Data):
        if data.name in self.data:
            self.read_set.add(data)
            print(f"{self.name} read {data.name}")
            return data.get_value()
        else:
            print(f"No Resource Found. {self.name} Can't Read Resource {data.name}")
            return None

    def write(self, data: Data, value):
        if data.name in self.data:
            data.set_value(value)
            self.write_set.add(data)
            print(f"{self.name} write new value ({value}) into {data.name}")
            return data
        else:
            print(f"No Resource Found. Can't Write Resource {data.name}")
            return None

    def commit(self):
        # Memproses operasi commit
        for resource in self.data.values():
            resource.set_committed_value()
        print(f"{self.name} commited")

    def abort(self):
        # Memproses operasi abort
        for resource in self.data.values():
            resource.reset_value()
