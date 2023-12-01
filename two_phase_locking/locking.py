class TwoPhaseLocking:
    def __init__(self, transaction_table, lock_table, type_locking):
        self.transaction_table = transaction_table
        self.lock_table = lock_table
        self.type_locking = type_locking

    def add_transaction(self,type):
        transaction = "T"+str(type[1])
        if transaction not in self.transaction_table["T"].values:
            row = {"T_ID":"T"+str(type[1]),"TimeStamp":type[1],"State": "Active","Blocked_by":[],"Blocked_Operations":[]}
            self.transaction_table = self.transaction_table.append(row,ignore_index=True)
            print("Begin Transaction: T"+str(type[1]))

    def abort(self,lock_table_row_index,current_transaction_row_index, comparing_transaction, row_index_of_comparing_transaction):

        if self.type_locking != "4":
            self.transaction_table.iloc[row_index_of_comparing_transaction]["State"] = "Aborted"
            self.transaction_table.iloc[row_index_of_comparing_transaction]["Blocked_by"] = []
            self.transaction_table.iloc[row_index_of_comparing_transaction]["Blocked_Operations"] = []
            self.lock_table.iloc[lock_table_row_index]["T_ID"].remove(comparing_transaction)
            temp = (self.lock_table["T_ID"].values)[:]
            for i in temp:
                if comparing_transaction in i:
                    location = list(temp).index(i)
                    (self.lock_table["T_ID"].values)[location].remove(comparing_transaction)
                    if not (self.lock_table["T_ID"].values)[location]:
                        data_item_row = (list(self.lock_table["T_ID"].values)).index((self.lock_table["T_ID"].values)[location])
                        self.lock_table = self.lock_table.drop(self.lock_table.index[data_item_row])
            self.lock_table = self.lock_table.reset_index(drop = True)
            if list(self.transaction_table["Blocked_by"].values):
                for j in self.transaction_table["Blocked_by"].values:
                    if comparing_transaction in j:
                        transaction_row = (list(self.transaction_table["Blocked_by"].values)).index(j)
                        j.remove(comparing_transaction)
                        self.transaction_table.iloc[transaction_row]['State'] = "Active"
                        for k in list(self.transaction_table.iloc[transaction_row]["Blocked_Operations"][:]):
                            self.run(k)
                        if self.transaction_table.iloc[transaction_row]["Blocked_by"] == []:
                            self.transaction_table.iloc[transaction_row]["Blocked_Operations"] = []
        elif self.type_locking == "4":
            self.transaction_table.iloc[current_transaction_row_index]["State"] = "Aborted"
            self.transaction_table.iloc[current_transaction_row_index]["Blocked_by"] = []
            self.transaction_table.iloc[current_transaction_row_index]["Blocked_Operations"] = []
            current_transaction = self.transaction_table.iloc[current_transaction_row_index]["T_ID"]
            temp = (self.lock_table["T_ID"].values)[:]
            for i in temp:
                if current_transaction in i:
                    location = list(temp).index(i)
                    (self.lock_table["T_ID"].values)[location].remove(current_transaction)
                    if not (self.lock_table["T_ID"].values)[location]:
                        data_item_row = (list(self.lock_table["T_ID"].values)).index((self.lock_table["T_ID"].values)[location])
                        self.lock_table = self.lock_table.drop(self.lock_table.index[data_item_row])
            self.lock_table = self.lock_table.reset_index(drop = True)
            if list(self.transaction_table["Blocked_by"].values):
                for j in self.transaction_table["Blocked_by"].values:
                    if current_transaction in j:
                        transaction_row = (list(self.transaction_table["Blocked_by"].values)).index(j)
                        j.remove(current_transaction)
                        self.transaction_table.iloc[transaction_row]['State'] = "Active"
                        for k in list(self.transaction_table.iloc[transaction_row]["Blocked_Operations"][:]):
                            self.run(k)
                        if self.transaction_table.iloc[transaction_row]["Blocked_by"] == []:
                            self.transaction_table.iloc[transaction_row]["Blocked_Operations"] = []
    
    def read_operation(self, type):
        if self.transaction_table.iloc[int(type[1])-1]["State"] == "Active":
            if type[3] in self.lock_table["Data-Item"].values:
                row_index = int(self.lock_table[self.lock_table["Data-Item"] == type[3]].index[0])
                if self.lock_table.iloc[row_index]["Lock-Mode"] == "R":
                    self.lock_table.iloc[row_index]["T_ID"].append("T"+str(type[1]))
                    print("Being a shared lock,","T"+str(type[1]),"also acquires the R Lock for",type[3])

                if self.lock_table.loc[row_index]["Lock-Mode"] == "W":
                    transaction = "T"+str(type[1])
                    current_transaction_row_index = int(self.transaction_table[self.transaction_table["T_ID"] == transaction].index[0])
                    transaction_timestamp = self.transaction_table.iloc[current_transaction_row_index]["TimeStamp"]

                    if self.type_locking == "1":
                        if transaction in self.lock_table.iloc[row_index]["T_ID"]:
                            self.lock_table.iloc[row_index]["Lock-Mode"] = type[0].upper()
                        else:
                            lock_holding_transaction = (self.lock_table.iloc[row_index]["T_ID"])[0]
                            row_index_of_comparing_transaction = int(self.transaction_table[self.transaction_table["T_ID"] == lock_holding_transaction].index[0])
                            timestamp_of_comparing_transaction = self.transaction_table.iloc[row_index_of_comparing_transaction]["TimeStamp"]
                            if transaction_timestamp < timestamp_of_comparing_transaction:
                                
                                self.abort(row_index,current_transaction_row_index,lock_holding_transaction,row_index_of_comparing_transaction)
                                print(lock_holding_transaction,"is aborted as it is younger than",transaction)
                            else:
                                    
                                self.transaction_table.iloc[current_transaction_row_index]["State"] = "Blocked"
                                self.transaction_table.iloc[current_transaction_row_index]["Blocked_by"].append(str(lock_holding_transaction))
                                if type not in self.transaction_table.iloc[current_transaction_row_index]["Blocked_Operations"]:
                                    self.transaction_table.iloc[current_transaction_row_index]["Blocked_Operations"].append(type)
                                print(transaction,"waits for the older transaction",lock_holding_transaction,"to release the lock for",type[3])

                            if not self.lock_table.iloc[row_index]["T_ID"]:
                                self.lock_table.iloc[row_index]["Lock-Mode"] = type[0].upper()
                                self.lock_table.iloc[row_index]["T_ID"].append(transaction)

                    elif self.type_locking == "2": # Wait-Die
                        if transaction in self.lock_table.iloc[row_index]["T_ID"]:
                            self.lock_table.iloc[row_index]["Lock-Mode"] = type[0].upper()
                        else:
                            lock_holding_transaction = (self.lock_table.iloc[row_index]["T_ID"])[0]
                            row_index_of_comparing_transaction = int(self.transaction_table[self.transaction_table["T_ID"] == lock_holding_transaction].index[0])
                            timestamp_of_comparing_transaction = self.transaction_table.iloc[row_index_of_comparing_transaction]["TimeStamp"]
                            if transaction_timestamp > timestamp_of_comparing_transaction:
                                self.abort(row_index,current_transaction_row_index,lock_holding_transaction,row_index_of_comparing_transaction)
                                print(lock_holding_transaction,"is aborted as it is older than",transaction)
                            else:
                                self.transaction_table.iloc[current_transaction_row_index]["State"] = "Blocked"
                                self.transaction_table.iloc[current_transaction_row_index]["Blocked_by"].append(str(lock_holding_transaction))
                                if type not in self.transaction_table.iloc[current_transaction_row_index]["Blocked_Operations"]:
                                    self.transaction_table.iloc[current_transaction_row_index]["Blocked_Operations"].append(type)
                            
                            if not self.lock_table.iloc[row_index]["T_ID"]:
                                self.lock_table.iloc[row_index]["Lock-Mode"] = type[0].upper()
                                self.lock_table.iloc[row_index]["T_ID"].append(transaction)
                    elif self.type_locking == "3":
                        # No-waiting
                        if transaction in self.lock_table.iloc[row_index]["T_ID"]:
                            self.lock_table.iloc[row_index]["Lock-Mode"] = type[0].upper()
                            
                        else:
                            lock_holding_transaction = (self.lock_table.iloc[row_index]["T_ID"])[0]
                            row_index_of_comparing_transaction = int(self.transaction_table[self.transaction_table["T_ID"] == lock_holding_transaction].index[0])
                            self.abort(row_index,current_transaction_row_index,lock_holding_transaction,row_index_of_comparing_transaction)
                            print(lock_holding_transaction,"is aborted and",transaction,"acquires the R lock for",type[3])
    
                            if not self.lock_table.iloc[row_index]["T_ID"]:
                                self.lock_table.iloc[row_index]["Lock-Mode"] = type[0].upper()
                                self.lock_table.iloc[row_index]["T_ID"].append(transaction)
                    elif self.type_locking == "4":
                        # Cautious-Waiting
                        if transaction in self.lock_table.iloc[row_index]["T_ID"]:
                            self.lock_table.iloc[row_index]["Lock-Mode"] = type[0].upper()
                            
                        else:
                            lock_holding_transaction = (self.lock_table.iloc[row_index]["T_ID"])[0]
                            row_index_of_comparing_transaction = int(self.transaction_table[self.transaction_table["T_ID"] == lock_holding_transaction].index[0])
                            state_of_lock_holding_transaction = self.transaction_table.iloc[row_index_of_comparing_transaction]["State"]
                            # Checks the state of the lock holding transaction
                            if state_of_lock_holding_transaction == "Blocked":
                                self.abort(row_index,current_transaction_row_index,lock_holding_transaction,row_index_of_comparing_transaction)
                                print(transaction,"is aborted as",lock_holding_transaction,"is blocked.")
                            else:
                                # Wait for the lock lock holding transaction
                                self.transaction_table.iloc[current_transaction_row_index]["State"] = "Blocked"
                                self.transaction_table.iloc[current_transaction_row_index]["Blocked_by"].append(str(lock_holding_transaction))
                                if type not in self.transaction_table.iloc[current_transaction_row_index]["Blocked_Operations"]:
                                    self.transaction_table.iloc[current_transaction_row_index]["Blocked_Operations"].append(type)
                                print(transaction,"waits for",lock_holding_transaction,"to release",type[3])
            else:
                add_row = {"Data-Item": str(type[3]), "Lock-Mode":type[0].upper(), "T_ID": ["T"+str(type[1])]}
                self.lock_table = self.lock_table.append(add_row, ignore_index=True)
                print("T"+str(type[1]),"acquires the R lock for", type[3])

        # Transaction is blocked, thus operation is appended to list of Blocked_Operations            
        if self.transaction_table.iloc[int(type[1])-1]["State"] == "Blocked" and type not in self.transaction_table.iloc[int(type[1])-1]["Blocked_Operations"]:
            row_index = int(self.transaction_table[self.transaction_table["T_ID"] == "T"+str(type[1])].index[0])
            self.transaction_table.iloc[row_index]["Blocked_Operations"].append(type)
            print("As", "T"+str(type[1]), "is blocked", type, "is appended to the list of Blocked_Operations.")
            
        if self.transaction_table.iloc[int(type[1])-1]["State"] == "Aborted":
            print("T"+str(type[1]),"is already aborted. So no changes in the tables.")
        
    def write_operation(self, type):
        if self.transaction_table.iloc[int(type[1])-1]["State"] == "Active":
            transaction = "T"+str(type[1])
            current_transaction_row_index = int(self.transaction_table[self.transaction_table["T_ID"] == transaction].index[0])
            transaction_timestamp = self.transaction_table.iloc[current_transaction_row_index]["TimeStamp"]
            if type[3] in self.lock_table["Data-Item"].values:
                row_index = int(self.lock_table[self.lock_table["Data-Item"] == type[3]].index[0])
                if self.type_locking == "1":
                    if transaction in self.lock_table.iloc[row_index]["T_ID"]:
                        self.lock_table.iloc[row_index]["T_ID"].remove(transaction)
                        if not self.lock_table.iloc[row_index]["T_ID"]:
                            self.lock_table.iloc[row_index]["Lock-Mode"] = type[0].upper()
                            self.lock_table.iloc[row_index]["T_ID"].append(transaction)
                            print(transaction, "upgrades R lock to W lock for", type[3])
                        else:
                            for i in list(self.lock_table.iloc[row_index]["T_ID"]):
                                row_index_of_comparing_transaction = int(self.transaction_table[self.transaction_table["T_ID"] == i].index[0])
                                timestamp_of_comparing_transaction = self.transaction_table.iloc[row_index_of_comparing_transaction]["TimeStamp"]
                                if transaction_timestamp < timestamp_of_comparing_transaction:
                                    self.abort(row_index,current_transaction_row_index,i,row_index_of_comparing_transaction)
                                    print(i,"is aborted as it is younger than",transaction)
                                else:
                                    self.transaction_table.iloc[current_transaction_row_index]["State"] = "Blocked"
                                    if i not in self.transaction_table.iloc[current_transaction_row_index]["Blocked_by"]:
                                        self.transaction_table.iloc[current_transaction_row_index]["Blocked_by"].append(str(i))
                                    if type not in self.transaction_table.iloc[current_transaction_row_index]["Blocked_Operations"]:
                                        self.transaction_table.iloc[current_transaction_row_index]["Blocked_Operations"].append(type)
                                    print(transaction,"is blocked by",i,"for",type[3])
                            if not self.lock_table.iloc[row_index]["T_ID"]:
                                self.lock_table.iloc[row_index]["Lock-Mode"] = type[0].upper()
                                self.lock_table.iloc[row_index]["T_ID"].append(transaction)
                                print(transaction,"acquires the W lock for",type[3])
                            else:
                                self.lock_table.iloc[row_index]["T_ID"].append(transaction)
                    
                    else:
                        for i in list(self.lock_table.iloc[row_index]["T_ID"]):
                            row_index_of_comparing_transaction = int(self.transaction_table[self.transaction_table["T_ID"] == i].index[0])
                            timestamp_of_comparing_transaction = self.transaction_table.iloc[row_index_of_comparing_transaction]["TimeStamp"]
                            if transaction_timestamp < timestamp_of_comparing_transaction:
                                self.abort(row_index,current_transaction_row_index,i,row_index_of_comparing_transaction)
                                print(i,"is aborted as it is younger than",transaction)
                                
                            else:
                                self.transaction_table.iloc[current_transaction_row_index]["State"] = "Blocked"
                                if i not in self.transaction_table.iloc[current_transaction_row_index]["Blocked_by"]:
                                    self.transaction_table.iloc[current_transaction_row_index]["Blocked_by"].append(str(i))
                                if type not in self.transaction_table.iloc[current_transaction_row_index]["Blocked_Operations"]:
                                    self.transaction_table.iloc[current_transaction_row_index]["Blocked_Operations"].append(type)
                        if not self.lock_table.iloc[row_index]["T_ID"]:
                            self.lock_table.iloc[row_index]["Lock-Mode"] = type[0].upper()
                            self.lock_table.iloc[row_index]["T_ID"].append(transaction)
                
                elif self.type_locking == "2":
                    if transaction in self.lock_table.iloc[row_index]["T_ID"]:
                        self.lock_table.iloc[row_index]["T_ID"].remove(transaction)
                        if not self.lock_table.iloc[row_index]["T_ID"]:
                            self.lock_table.iloc[row_index]["Lock-Mode"] = type[0].upper()
                            self.lock_table.iloc[row_index]["T_ID"].append(transaction)
                        else:
                            for i in list(self.lock_table.iloc[row_index]["T_ID"]):
                                row_index_of_comparing_transaction = int(self.transaction_table[self.transaction_table["T_ID"] == i].index[0])
                                timestamp_of_comparing_transaction = self.transaction_table.iloc[row_index_of_comparing_transaction]["TimeStamp"]
                                if transaction_timestamp > timestamp_of_comparing_transaction:
                                    self.abort(row_index,current_transaction_row_index,i,row_index_of_comparing_transaction)
                                    print(i,"is aborted as it is older than",transaction)
                                    
                                else:
                                    self.transaction_table.iloc[current_transaction_row_index]["State"] = "Blocked"
                                    if i not in self.transaction_table.iloc[current_transaction_row_index]["Blocked_by"]:
                                        self.transaction_table.iloc[current_transaction_row_index]["Blocked_by"].append(str(i))
                                    if type not in self.transaction_table.iloc[current_transaction_row_index]["Blocked_Operations"]:
                                        self.transaction_table.iloc[current_transaction_row_index]["Blocked_Operations"].append(type)
                                    print(transaction,"is blocked by",i,"for",type[3])
                            if not self.lock_table.iloc[row_index]["T_ID"]:
                                self.lock_table.iloc[row_index]["Lock-Mode"] = type[0].upper()
                                self.lock_table.iloc[row_index]["T_ID"].append(transaction)
                                print(transaction,"acquires the W lock for",type[3])
                            else:
                                self.lock_table.iloc[row_index]["T_ID"].append(transaction)
                    
                    else:
                        for i in list(self.lock_table.iloc[row_index]["T_ID"]):
                            row_index_of_comparing_transaction = int(self.transaction_table[self.transaction_table["T_ID"] == i].index[0])
                            timestamp_of_comparing_transaction = self.transaction_table.iloc[row_index_of_comparing_transaction]["TimeStamp"]
                            if transaction_timestamp > timestamp_of_comparing_transaction:
                                self.abort(row_index,current_transaction_row_index,i,row_index_of_comparing_transaction)
                            else:
                                self.transaction_table.iloc[current_transaction_row_index]["State"] = "Blocked"
                                self.transaction_table.iloc[current_transaction_row_index]["Blocked_by"].append(str(i))
                                if type not in self.transaction_table.iloc[current_transaction_row_index]["Blocked_Operations"]:
                                    self.transaction_table.iloc[current_transaction_row_index]["Blocked_Operations"].append(type)
                        if not self.lock_table.iloc[row_index]["T_ID"]:
                            self.lock_table.iloc[row_index]["Lock-Mode"] = type[0].upper()
                            self.lock_table.iloc[row_index]["T_ID"].append(transaction)
                elif self.type_locking == "3":
                    for i in list(self.lock_table.iloc[row_index]["T_ID"]):
                        if i != transaction:
                            row_index_of_comparing_transaction = int(self.transaction_table[self.transaction_table["T_ID"] == i].index[0])
                            timestamp_of_comparing_transaction = self.transaction_table.iloc[row_index_of_comparing_transaction]["TimeStamp"]
                            self.abort(row_index,current_transaction_row_index,i,row_index_of_comparing_transaction)
                    self.lock_table.iloc[row_index]["Lock-Mode"] = type[0].upper()
                elif self.type_locking == "4":
                    if transaction in self.lock_table.iloc[row_index]["T_ID"]  and len(list(self.lock_table.iloc[row_index]["T_ID"])) == 1:
                        self.lock_table.iloc[row_index]["Lock-Mode"] = type[0].upper()
                        print(transaction,"upgrades to W lock on",type[3])

                    else:
                        for i in list(self.lock_table.iloc[row_index]["T_ID"]):
                            if i != transaction:
                                row_index_of_comparing_transaction = int(self.transaction_table[self.transaction_table["T_ID"] == i].index[0])
                                status_of_current_transaction = self.transaction_table.iloc[row_index]["State"]
                                status_of_comparing_transaction = self.transaction_table.iloc[row_index_of_comparing_transaction]["State"]
                                if status_of_comparing_transaction == "Blocked" and status_of_current_transaction != "Aborted":
                                    print(transaction,"is aborted and locks are released as",i,"is blocked.")
                                    self.abort(row_index,current_transaction_row_index,i,row_index_of_comparing_transaction)
                                    

                                elif status_of_comparing_transaction == "Active" and status_of_current_transaction != "Aborted":
                                    self.transaction_table.iloc[current_transaction_row_index]["State"] = "Blocked"
                                    if i not in self.transaction_table.iloc[current_transaction_row_index]["Blocked_by"]:
                                        self.transaction_table.iloc[current_transaction_row_index]["Blocked_by"].append(str(i))
                                    if type not in self.transaction_table.iloc[current_transaction_row_index]["Blocked_Operations"]:
                                        self.transaction_table.iloc[current_transaction_row_index]["Blocked_Operations"].append(type)
                                    print(transaction,"waits for",i,"to release",type[3])
            else:
                add_row = {"Data-Item": str(type[3]), "Lock-Mode":type[0].upper(), "T_ID": ["T"+str(type[1])]}
                self.lock_table = self.lock_table.append(add_row, ignore_index=True)
                
        if self.transaction_table.iloc[int(type[1])-1]["State"] == "Blocked" and type not in self.transaction_table.iloc[int(type[1])-1]["Blocked_Operations"]:
            row_index = int(self.transaction_table[self.transaction_table["T_ID"] == "T"+str(type[1])].index[0])
            self.transaction_table.iloc[row_index]["Blocked_Operations"].append(type)
            print("As","T"+str(type[1]), "is blocked operation!") 
            print("So, It was added to the list to Blocked_Operations.")
        
        if self.transaction_table.iloc[int(type[1])-1]["State"] == "Aborted" and self.type_locking != "4":
            print("T"+str(type[1]),"is already aborted!")
            print("No changes in the tables.")
    
    def end_transaction(self,type):
        transaction = "T"+str(type[1])
        if self.transaction_table.iloc[int(type[1])-1]["State"] == "Active":
            print(transaction,"The transaction was successfully committed and the corresponding locks were released.")
            self.transaction_table.iloc[int(type[1])-1]["State"] = "Committed"
            for i in list(self.lock_table["T_ID"].values):
                if transaction in i:
                    i.remove(transaction)
                    if not i:
                        data_item_row = (list(self.lock_table["T_ID"].values)).index(i)
                        self.lock_table = self.lock_table.drop(self.lock_table.index[data_item_row])
            self.lock_table = self.lock_table.reset_index(drop = True)

            if list(self.transaction_table["Blocked_by"].values):
                for j in self.transaction_table["Blocked_by"].values:
                    if transaction in j:
                        transaction_row = (list(self.transaction_table["Blocked_by"].values)).index(j)
                        j.remove(transaction)
                        self.transaction_table.iloc[transaction_row]['State'] = "Active"
                        for k in list(self.transaction_table.iloc[transaction_row]["Blocked_Operations"][:]):
                            self.run(k)
                        if self.transaction_table.iloc[transaction_row]["Blocked_by"] == []:
                            self.transaction_table.iloc[transaction_row]["Blocked_Operations"] = []
        
        if self.transaction_table.iloc[int(type[1])-1]["State"] == "Blocked" and not (type in self.transaction_table.iloc[int(type[1])-1]["Blocked_Operations"]):
            row_index = int(self.transaction_table[self.transaction_table["T_ID"] == "T"+str(type[1])].index[0])
            self.transaction_table.iloc[row_index]["Blocked_Operations"].append(type)

        if self.transaction_table.iloc[int(type[1])-1]["State"] == "Aborted":
            print(transaction,"is already aborted!")
            print("No changes in the tables.")

    def run(self,operation):
        if operation[0] == "b":
            self.add_transaction(operation)
        if operation[0] == "r":
            self.read_operation(operation)
        if operation[0] == "w":
            self.write_operation(operation)
        if operation[0] == "e":
            self.end_transaction(operation)