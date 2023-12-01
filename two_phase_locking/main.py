import pandas as pd
from locking import *
from tabulate import tabulate

filename = "input-1.txt"
print("Current file:",filename)
transaction_table = pd.DataFrame(columns=["T_ID", "TimeStamp","State", "Blocked_by", "Blocked_Operations"])
lock_table = pd.DataFrame(columns=["Data-Item","Lock-Mode","T_ID"])
proceed = True
while proceed:
    type_locking = input("Please two phase locking type that want to test:\n1. Wound & Wait\n2. Wait & Die\n3. No-Waiting\n4. Cautious Waiting\nSelect 1,2,3 or 4.\nType here: ")
    locking_protocol = TwoPhaseLocking(transaction_table,lock_table,type_locking)
    if type_locking == "1" or type_locking == "2" or type_locking == "3" or type_locking == "4":
        file = open(filename,"r")
        for line in file:
            line = line.rstrip("\n")
            print("Operation", line)
            if line[0] == "b":
                locking_protocol.add_transaction(line)
            if line[0] == "r":
                locking_protocol.read_operation(line)
            if line[0] == "w":
                locking_protocol.write_operation(line)
            if line[0] == "e":
                locking_protocol.end_transaction(line)
            print("Transaction Table:")
            print(tabulate(transaction_table, headers='keys', tablefmt='psql',showindex="never"))
            print("Lock Table:")
            print(tabulate(lock_table, headers='keys', tablefmt='psql',showindex="never"))
        proceed = False
    else:
        print("\n\n---------Please select again!")