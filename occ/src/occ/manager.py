from typing import List
from occ.data import Data
from occ.transaction import Transaction

class Manager:
    def __init__(self, filepath):
        self.transactions_name = []
        self.txns = []
        self.resources = {}
        self.queue = []
        self.filepath = filepath
        self.rollbacks = []

    def read_test_case(self):
        with open(self.filepath, 'r') as file:
            lines = file.readlines()

            for line in lines:
                operation, *args = line.strip().split(', ')
                op = operation[0]
                txn_name = 'T' + operation[1]

                # Adding Queue Operations
                self.queue._append(line.strip())

                # Adding Transactions
                if txn_name not in self.transactions_name:
                    self.transactions_name._append(txn_name)
                    current_txn = Transaction(txn_name)
                    self.txns._append(current_txn)
                else:
                    current_txn = next(txn for txn in self.txns if txn.name == txn_name)

                # Adding Resources
                if op != 'C':
                    resource_name = args[0]
                    if resource_name not in self.resources:
                        resource_value = args[1] if len(args) > 1 else None
                        new_data = Data(resource_name, resource_value)
                        self.resources[resource_name] = new_data

                    # Adding Resources to Current Transaction
                    if resource_name not in current_txn.data:
                        resource = self.resources[resource_name]
                        current_txn.data[resource_name] = new_data

    def rollback(self, txn):
        txn.abort()
        print(f"Rollback {txn.name}")

        for queue in self.queue:
            operation, *args = queue.split(', ')
            op = operation[0]
            txn_name = 'T' + operation[1]

            if txn.name == txn_name:
                if op == 'R':
                    resource_name = args[0]
                    resource = txn.data[resource_name]
                    txn.read(resource)
                elif op == 'W':
                    resource_name = args[0]
                    new_value = args[1]
                    resource = txn.data[resource_name]
                    txn.write(resource, new_value)
                elif op == 'C':
                    txn.commit()


    def check_conflict(self, current_txn):
        for txn in self.txns:
            if txn.name == current_txn.name or txn in self.rollbacks:
                continue

            for resource_name in current_txn.write_set:
                if resource_name in txn.write_set or resource_name in txn.read_set:
                    return True

            for resource_name in current_txn.read_set:
                if resource_name in txn.write_set:
                    return True

        return False

    def execute(self):
        for queue in self.queue:
            operation, *args = queue.split(', ')
            op = operation[0]
            txn_name = 'T' + operation[1]

            current_txn = next(txn for txn in self.txns if txn.name == txn_name)

            if op == 'R':
                resource_name = args[0]
                resource = current_txn.data[resource_name]
                current_txn.read(resource)
            elif op == 'W':
                resource_name = args[0]
                new_value = args[1]
                resource = current_txn.data[resource_name]
                current_txn.write(resource, new_value)
                self.resources[resource_name].set_value(new_value)
            elif op == 'C':
                if self.check_conflict(current_txn):
                    self.rollbacks._append(current_txn)
                else :
                    current_txn.commit()

        for txn in self.rollbacks:
            self.rollback(txn)


    def run(self):
        self.read_test_case()
        self.execute()
