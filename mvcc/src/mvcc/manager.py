from mvcc.resource import Resource
from mvcc.transaction import *
from const import *
from util import *

import re
import logging

class Manager:
    def __init__(self):
        self._transaction = {}
        self._queue = []
        self._resource = {}
        self._read_logs = {}
        self._write_logs = {}

    def is_rollback(self, txn):
        return self.get_txn(txn)._status == Status.ABORT

    def set_read(self, txn, vers, res):
        self._read_logs.setdefault(res, []).append((txn, vers))

    def set_write(self, txn, res):
        self._write_logs.setdefault(txn, []).append((res, txn))

    def get_txn(self, key):
        return self._transaction.get(key, False)

    def get_vers(self, key):
        return self._resource[key]._version

    def print_transaction(self):
        for key, txn in self._transaction.items():
            print(txn)

    def check_write(self, txn):
        return self._write_logs.get(txn, [])

    def check_read(self, written):
        read = []
        for res, vers in written:
            read.extend(txn for txn, read_vers in self._read_logs.get(res, []) if read_vers == vers)
        return read

    def get_aborted(self):
        return [key for key, txn in self._transaction.items() if txn._status == Status.ABORT]

    def rollback(self, read):
        if not read:
            return
        for txn in read:
            logging.info(f"[ABORT] T{txn} Cascading Rollback")
            self._transaction[txn].abort()
            self.rollback(self.check_read(self.check_write(txn)))

    def get_max(self, res, txn):
        curr = None
        for vers, res_ts in self.get_vers(res).items():
            if res_ts[1] <= txn:
                curr = res_ts[0], res_ts[1], vers
        return curr

    def req_write(self, res, txn, val):
        qk = self.get_max(res, txn)
        if txn < qk[0]:
            logging.info(f"[>] ABORT T{txn}")
            self.get_txn(txn).abort()
            self.rollback(self.check_read(self.check_write(txn)))
        elif txn == qk[1]:
            logging.info(f"[>] OVERWRITE Value({res}{txn}) = {val}")
            self._resource[res]._version[qk[2]][2] = val
            self.set_write(txn, res)
        else:
            logging.info(f"[>] WRITE {res}{txn} = ({txn}, {txn}, {val})")
            self._resource[res]._version[txn] = [txn, txn, val]
            self.set_write(txn, res)

    def req_read(self, res, txn):
        qk = self.get_max(res, txn)
        logging.info(f"[>] READ Value {res}{qk[2]}:{self.get_vers(res)[qk[2]][2]}")
        if txn > qk[0]:
            self.get_vers(res)[qk[2]][0] = txn
            logging.info(f"[>] Update R-TS({res}{qk[2]}) -> ({self.get_vers(res)[qk[2]][0]}, {self.get_vers(res)[qk[2]][1]}, {self.get_vers(res)[qk[2]][2]})")
        self.set_read(txn, qk[2], res)

    def run(self):
        logging.basicConfig(format=" %(message)s", level=logging.INFO)
        while self._queue:
            logging.info(f"{self._queue[0]}")
            if int(getNumber(self._queue[0])) in self.get_aborted():
                logging.info(
                    f"[WARNING]: T{getNumber(self._queue[0])} already rolled back, skipped "
                )
                self._queue.pop(0)
            else:
                if not self.get_txn(getNumber(self._queue[0])):
                    txn = Transaction(
                        getNumber(self._queue[0]),
                        getNumber(self._queue[0]),
                    )
                    self._transaction[getNumber(self._queue[0])] = txn

                if re.search(Pattern.WRITE, self._queue[0], re.IGNORECASE):
                    params = [s.strip() for s in getParam(self._queue[0])]
                    self.handle_write(params)

                elif re.search(Pattern.READ, self._queue[0], re.IGNORECASE):
                    params = [s.strip() for s in getParam(self._queue[0])]
                    self.handle_read(params)

                elif re.search(Pattern.COMMIT, self._queue[0], re.IGNORECASE):
                    self._transaction[getNumber(self._queue[0])].commit()
                self._queue.pop(0)

        for txn in self._transaction.values():
            print(txn)

    def handle_write(self, params):
        if params[1] not in self._resource:
            self._resource[params[1]] = Resource(params[1])
        self.req_write(params[1], getNumber(params[0]), params[2])

    def handle_read(self, params):
        if params[1] not in self._resource:
            self._resource[params[1]] = Resource(params[1])
        self.req_read(params[1], getNumber(params[0]))
