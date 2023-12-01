class TwoPhaseLocking:
    def __init__(self, transactions, lock_table, type_locking):
        self.transactions = transactions
        self.lock_table = lock_table
        self.type_locking = type_locking

    def insert_transaction(self,type):
        transaction = "Transaction"+str(type[1])
        if transaction not in self.transactions["Transaction_id"].values:
            row = {"Transaction_id":"Transaction"+str(type[1]),"TimeStamp":type[1],"State": "Active","Blocked_by":[],"Blocked_Operations":[]}
            self.transactions = self.transactions._append(row,ignore_index=True)
            print("Begin Transaction: T"+str(type[1]))

    def abort(self,lock_table_row_index, comp_transaction, row_index_comp):
        self.transactions.iloc[row_index_comp]["State"] = "Aborted"
        self.transactions.iloc[row_index_comp]["Blocked_by"] = []
        self.transactions.iloc[row_index_comp]["Blocked_Operations"] = []
        self.lock_table.iloc[lock_table_row_index]["Transaction_id"].remove(comp_transaction)
        temp = (self.lock_table["Transaction_id"].values)[:]
        for i in temp:
            if comp_transaction in i:
                location = list(temp).index(i)
                (self.lock_table["Transaction_id"].values)[location].remove(comp_transaction)
                if not (self.lock_table["Transaction_id"].values)[location]:
                    data_item_row = (list(self.lock_table["Transaction_id"].values)).index((self.lock_table["Transaction_id"].values)[location])
                    self.lock_table = self.lock_table.drop(self.lock_table.index[data_item_row])
        self.lock_table = self.lock_table.reset_index(drop = True)
        if list(self.transactions["Blocked_by"].values):
            for j in self.transactions["Blocked_by"].values:
                if comp_transaction in j:
                    transaction_row = (list(self.transactions["Blocked_by"].values)).index(j)
                    j.remove(comp_transaction)
                    self.transactions.iloc[transaction_row]['State'] = "Active"
                    for k in list(self.transactions.iloc[transaction_row]["Blocked_Operations"][:]):
                        self.run(k)
                    if self.transactions.iloc[transaction_row]["Blocked_by"] == []:
                        self.transactions.iloc[transaction_row]["Blocked_Operations"] = []
    
    def setLockMode(self,type,row_index):
        self.lock_table.iloc[row_index]["Lock-Mode"] = type[0].upper()

    def read_wound_wait(self,type,transaction,transaction_timestamp,row_index,curr_transaction_index,lock_holding_transaction,row_index_comp):
        timestamp_of_comparing_transaction = self.transactions.iloc[row_index_comp]["TimeStamp"]
        if transaction_timestamp < timestamp_of_comparing_transaction:
            self.abort(row_index,lock_holding_transaction,row_index_comp)
            print(lock_holding_transaction,"is aborted") 
            print("It is younger than ",transaction)
        else:
                
            self.transactions.iloc[curr_transaction_index]["State"] = "Blocked"
            self.transactions.iloc[curr_transaction_index]["Blocked_by"].append(str(lock_holding_transaction))
            if type not in self.transactions.iloc[curr_transaction_index]["Blocked_Operations"]:
                self.transactions.iloc[curr_transaction_index]["Blocked_Operations"].append(type)
            print(transaction,"waits for the older transaction",lock_holding_transaction,"to release the lock for",type[3])

        if not self.lock_table.iloc[row_index]["Transaction_id"]:
            self.lock_table.iloc[row_index]["Lock-Mode"] = type[0].upper()
            self.lock_table.iloc[row_index]["Transaction_id"].append(transaction)

    def read_wait_die(self,type,transaction,transaction_timestamp,row_index,curr_transaction_index,lock_holding_transaction,row_index_comp):
        timestamp_of_comparing_transaction = self.transactions.iloc[row_index_comp]["TimeStamp"]
        if transaction_timestamp > timestamp_of_comparing_transaction:
            self.abort(row_index,lock_holding_transaction,row_index_comp)
            print(lock_holding_transaction,"is aborted")
            print("It is older than",transaction)
        else:
            self.transactions.iloc[curr_transaction_index]["State"] = "Blocked"
            self.transactions.iloc[curr_transaction_index]["Blocked_by"].append(str(lock_holding_transaction))
            if type not in self.transactions.iloc[curr_transaction_index]["Blocked_Operations"]:
                self.transactions.iloc[curr_transaction_index]["Blocked_Operations"].append(type)
        
        if not self.lock_table.iloc[row_index]["Transaction_id"]:
            self.lock_table.iloc[row_index]["Lock-Mode"] = type[0].upper()
            self.lock_table.iloc[row_index]["Transaction_id"].append(transaction)
    
    def read_no_waiting(self,type,transaction,row_index,lock_holding_transaction,row_index_comp):
        self.abort(row_index,lock_holding_transaction,row_index_comp)
        print(lock_holding_transaction,"is aborted and",transaction,"acquires the R lock for",type[3])
        if not self.lock_table.iloc[row_index]["Transaction_id"]:
            self.lock_table.iloc[row_index]["Lock-Mode"] = type[0].upper()
            self.lock_table.iloc[row_index]["Transaction_id"].append(transaction)

    def read_transaction(self, type):
        if self.transactions.iloc[int(type[1])-1]["State"] == "Active":
            if type[3] in self.lock_table["Data-Item"].values:
                row_index = int(self.lock_table[self.lock_table["Data-Item"] == type[3]].index[0])
                if self.lock_table.iloc[row_index]["Lock-Mode"] == "R":
                    self.lock_table.iloc[row_index]["Transaction_id"].append("Transaction"+str(type[1]))
                    print("Being a shared lock")
                    print("Transaction"+str(type[1]))
                    print("also acquires the R Lock for",type[3])

                if self.lock_table.loc[row_index]["Lock-Mode"] == "W":
                    transaction = "Transaction"+str(type[1])
                    curr_transaction_index = int(self.transactions[self.transactions["Transaction_id"] == transaction].index[0])
                    transaction_timestamp = self.transactions.iloc[curr_transaction_index]["TimeStamp"]

                    if transaction in self.lock_table.iloc[row_index]["Transaction_id"]:
                        self.setLockMode(type,row_index)
                    else:
                        lock_holding_transaction = (self.lock_table.iloc[row_index]["Transaction_id"])[0]
                        row_index_comp = int(self.transactions[self.transactions["Transaction_id"] == lock_holding_transaction].index[0])
                        if self.type_locking == "1": # No waiting
                            self.read_no_waiting(type,transaction,row_index,lock_holding_transaction,row_index_comp)
                        elif self.type_locking == "2": # Wait-Die
                            self.read_wait_die(type,transaction,transaction_timestamp,row_index,curr_transaction_index,lock_holding_transaction,row_index_comp)
                        elif self.type_locking == "3": # Wound-Wait
                            self.read_wound_wait(type,transaction,transaction_timestamp,row_index,curr_transaction_index,lock_holding_transaction,row_index_comp)
            else:
                add_row = {"Data-Item": str(type[3]), "Lock-Mode":type[0].upper(), "Transaction_id": ["Transaction"+str(type[1])]}
                self.lock_table = self.lock_table._append(add_row, ignore_index=True)
                print("Transaction"+str(type[1]), " acquires the R lock for", type[3])

        # Transaction is blocked, thus operation is appended to list of Blocked_Operations            
        if self.transactions.iloc[int(type[1])-1]["State"] == "Blocked" and type not in self.transactions.iloc[int(type[1])-1]["Blocked_Operations"]:
            row_index = int(self.transactions[self.transactions["Transaction_id"] == "Transaction"+str(type[1])].index[0])
            self.transactions.iloc[row_index]["Blocked_Operations"].append(type)
            print("As", "Transaction"+str(type[1]), "is blocked", type, "is appended to the list of Blocked_Operations.")
            
        if self.transactions.iloc[int(type[1])-1]["State"] == "Aborted":
            print("Transaction"+str(type[1]),"is already aborted. So no changes in the tables.")
    
    def write_wound_wait(self,type,transaction,row_index,transaction_timestamp,curr_transaction_index):
        if transaction in self.lock_table.iloc[row_index]["Transaction_id"]:
            self.lock_table.iloc[row_index]["Transaction_id"].remove(transaction)
            if not self.lock_table.iloc[row_index]["Transaction_id"]:
                self.lock_table.iloc[row_index]["Lock-Mode"] = type[0].upper()
                self.lock_table.iloc[row_index]["Transaction_id"].append(transaction)
                print(transaction, "upgrades R lock to W lock for", type[3])
            else:
                for i in list(self.lock_table.iloc[row_index]["Transaction_id"]):
                    row_index_comp = int(self.transactions[self.transactions["Transaction_id"] == i].index[0])
                    timestamp_of_comparing_transaction = self.transactions.iloc[row_index_comp]["TimeStamp"]
                    if transaction_timestamp < timestamp_of_comparing_transaction:
                        self.abort(row_index,i,row_index_comp)
                        print(i,"is aborted as it is younger than",transaction)
                    else:
                        self.transactions.iloc[curr_transaction_index]["State"] = "Blocked"
                        if i not in self.transactions.iloc[curr_transaction_index]["Blocked_by"]:
                            self.transactions.iloc[curr_transaction_index]["Blocked_by"].append(str(i))
                        if type not in self.transactions.iloc[curr_transaction_index]["Blocked_Operations"]:
                            self.transactions.iloc[curr_transaction_index]["Blocked_Operations"].append(type)
                        print(transaction,"is blocked by",i,"for",type[3])
                if not self.lock_table.iloc[row_index]["Transaction_id"]:
                    self.lock_table.iloc[row_index]["Lock-Mode"] = type[0].upper()
                    self.lock_table.iloc[row_index]["Transaction_id"].append(transaction)
                    print(transaction,"acquires the W lock for",type[3])
                else:
                    self.lock_table.iloc[row_index]["Transaction_id"].append(transaction)
        
        else:
            for i in list(self.lock_table.iloc[row_index]["Transaction_id"]):
                row_index_comp = int(self.transactions[self.transactions["Transaction_id"] == i].index[0])
                timestamp_of_comparing_transaction = self.transactions.iloc[row_index_comp]["TimeStamp"]
                if transaction_timestamp < timestamp_of_comparing_transaction:
                    self.abort(row_index,i,row_index_comp)
                    print(i,"is aborted as it is younger than",transaction)
                    
                else:
                    self.transactions.iloc[curr_transaction_index]["State"] = "Blocked"
                    if i not in self.transactions.iloc[curr_transaction_index]["Blocked_by"]:
                        self.transactions.iloc[curr_transaction_index]["Blocked_by"].append(str(i))
                    if type not in self.transactions.iloc[curr_transaction_index]["Blocked_Operations"]:
                        self.transactions.iloc[curr_transaction_index]["Blocked_Operations"].append(type)
            if not self.lock_table.iloc[row_index]["Transaction_id"]:
                self.lock_table.iloc[row_index]["Lock-Mode"] = type[0].upper()
                self.lock_table.iloc[row_index]["Transaction_id"].append(transaction)

    def write_wait_die(self,type,transaction,row_index,transaction_timestamp,curr_transaction_index):
        if transaction in self.lock_table.iloc[row_index]["Transaction_id"]:
            self.lock_table.iloc[row_index]["Transaction_id"].remove(transaction)
            if not self.lock_table.iloc[row_index]["Transaction_id"]:
                self.lock_table.iloc[row_index]["Lock-Mode"] = type[0].upper()
                self.lock_table.iloc[row_index]["Transaction_id"].append(transaction)
            else:
                for i in list(self.lock_table.iloc[row_index]["Transaction_id"]):
                    row_index_comp = int(self.transactions[self.transactions["Transaction_id"] == i].index[0])
                    timestamp_of_comparing_transaction = self.transactions.iloc[row_index_comp]["TimeStamp"]
                    if transaction_timestamp > timestamp_of_comparing_transaction:
                        self.abort(row_index,i,row_index_comp)
                        print(i,"is aborted as it is older than",transaction)
                        
                    else:
                        self.transactions.iloc[curr_transaction_index]["State"] = "Blocked"
                        if i not in self.transactions.iloc[curr_transaction_index]["Blocked_by"]:
                            self.transactions.iloc[curr_transaction_index]["Blocked_by"].append(str(i))
                        if type not in self.transactions.iloc[curr_transaction_index]["Blocked_Operations"]:
                            self.transactions.iloc[curr_transaction_index]["Blocked_Operations"].append(type)
                        print(transaction,"is blocked by",i,"for",type[3])
                if not self.lock_table.iloc[row_index]["Transaction_id"]:
                    self.lock_table.iloc[row_index]["Lock-Mode"] = type[0].upper()
                    self.lock_table.iloc[row_index]["Transaction_id"].append(transaction)
                    print(transaction,"acquires the W lock for",type[3])
                else:
                    self.lock_table.iloc[row_index]["Transaction_id"].append(transaction)
        
        else:
            for i in list(self.lock_table.iloc[row_index]["Transaction_id"]):
                row_index_comp = int(self.transactions[self.transactions["Transaction_id"] == i].index[0])
                timestamp_of_comparing_transaction = self.transactions.iloc[row_index_comp]["TimeStamp"]
                if transaction_timestamp > timestamp_of_comparing_transaction:
                    self.abort(row_index,i,row_index_comp)
                else:
                    self.transactions.iloc[curr_transaction_index]["State"] = "Blocked"
                    self.transactions.iloc[curr_transaction_index]["Blocked_by"].append(str(i))
                    if type not in self.transactions.iloc[curr_transaction_index]["Blocked_Operations"]:
                        self.transactions.iloc[curr_transaction_index]["Blocked_Operations"].append(type)
            if not self.lock_table.iloc[row_index]["Transaction_id"]:
                self.lock_table.iloc[row_index]["Lock-Mode"] = type[0].upper()
                self.lock_table.iloc[row_index]["Transaction_id"].append(transaction)

    def write_no_waiting(self,type,transaction,row_index):
        for i in list(self.lock_table.iloc[row_index]["Transaction_id"]):
            if i != transaction:
                row_index_comp = int(self.transactions[self.transactions["Transaction_id"] == i].index[0])
                timestamp_of_comparing_transaction = self.transactions.iloc[row_index_comp]["TimeStamp"]
                self.abort(row_index,i,row_index_comp)
        self.lock_table.iloc[row_index]["Lock-Mode"] = type[0].upper()

    def write_transaction(self, type):
        if self.transactions.iloc[int(type[1])-1]["State"] == "Active":
            transaction = "Transaction"+str(type[1])
            curr_transaction_index = int(self.transactions[self.transactions["Transaction_id"] == transaction].index[0])
            transaction_timestamp = self.transactions.iloc[curr_transaction_index]["TimeStamp"]
            if type[3] in self.lock_table["Data-Item"].values:
                row_index = int(self.lock_table[self.lock_table["Data-Item"] == type[3]].index[0])
                if self.type_locking == "1":
                    self.write_no_waiting(type,transaction,row_index)
                elif self.type_locking == "2":
                    self.write_wait_die(type,transaction,row_index,transaction_timestamp,curr_transaction_index)
                elif self.type_locking == "3":
                    self.write_wound_wait(type,transaction,row_index,transaction_timestamp,curr_transaction_index)
            else:
                add_row = {"Data-Item": str(type[3]), "Lock-Mode":type[0].upper(), "Transaction_id": ["Transaction"+str(type[1])]}
                self.lock_table = self.lock_table._append(add_row, ignore_index=True)
                
        if self.transactions.iloc[int(type[1])-1]["State"] == "Blocked" and type not in self.transactions.iloc[int(type[1])-1]["Blocked_Operations"]:
            row_index = int(self.transactions[self.transactions["Transaction_id"] == "Transaction"+str(type[1])].index[0])
            self.transactions.iloc[row_index]["Blocked_Operations"].append(type)
            print("As Transaction"+str(type[1]), "is blocked operation!") 
            print("So, It was added to the list to Blocked_Operations.")
        
        if self.transactions.iloc[int(type[1])-1]["State"] == "Aborted":
            print("Transaction"+str(type[1]),"is already aborted!")
            print("No changes in the tables.")

    def kill_transaction(self,type):
        transaction = "Transaction"+str(type[1])
        if self.transactions.iloc[int(type[1])-1]["State"] == "Active":
            print(transaction,"The transaction was successfully committed and the corresponding locks were released.")
            self.transactions.iloc[int(type[1])-1]["State"] = "Committed"
            for i in list(self.lock_table["Transaction_id"].values):
                if transaction in i:
                    i.remove(transaction)
                    if not i:
                        data_item_row = (list(self.lock_table["Transaction_id"].values)).index(i)
                        self.lock_table = self.lock_table.drop(self.lock_table.index[data_item_row])
            self.lock_table = self.lock_table.reset_index(drop = True)
            if list(self.transactions["Blocked_by"].values):
                for j in self.transactions["Blocked_by"].values:
                    if transaction in j:
                        transaction_row = (list(self.transactions["Blocked_by"].values)).index(j)
                        j.remove(transaction)
                        self.transactions.iloc[transaction_row]['State'] = "Active"
                        for k in list(self.transactions.iloc[transaction_row]["Blocked_Operations"][:]):
                            self.run(k)
                        if self.transactions.iloc[transaction_row]["Blocked_by"] == []:
                            self.transactions.iloc[transaction_row]["Blocked_Operations"] = []
        
        if self.transactions.iloc[int(type[1])-1]["State"] == "Blocked" and not (type in self.transactions.iloc[int(type[1])-1]["Blocked_Operations"]):
            row_index = int(self.transactions[self.transactions["Transaction_id"] == "Transaction"+str(type[1])].index[0])
            self.transactions.iloc[row_index]["Blocked_Operations"].append(type)

        if self.transactions.iloc[int(type[1])-1]["State"] == "Aborted":
            print(transaction,"is already aborted!")
            print("No changes in the tables.")

    def run(self,operation):
        if operation[0] == "b":
            self.insert_transaction(operation)
        if operation[0] == "r":
            self.read_transaction(operation)
        if operation[0] == "w":
            self.write_transaction(operation)
        if operation[0] == "e":
            self.kill_transaction(operation)