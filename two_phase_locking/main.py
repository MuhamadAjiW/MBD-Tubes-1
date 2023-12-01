import pandas as pd
from locking import *

filename = "test/test1.txt"
print("Current file:",filename)
transactions = pd.DataFrame(columns=["Transaction_id", "TimeStamp","State", "Blocked_by", "Blocked_Operations"])
lock_table = pd.DataFrame(columns=["Data-Item","Lock-Mode","Transaction_id"])
proceed = True
while proceed:
    type_locking = input("Please select two phase locking type that want to test:\n1. No-Waiting\n2. Wait & Die\n3. Wound & Wait\nSelect 1,2 or 3.\nType here: ")
    locking_protocol = TwoPhaseLocking(transactions,lock_table,type_locking)
    if type_locking == "1" or type_locking == "2" or type_locking == "3":
        file = open(filename,"r")
        for operation in file:
            operation = operation.rstrip("\n")
            print("Operation", operation)
            locking_protocol.run(operation)
        proceed = False
    else:
        print("\n\n---------Please select again!")