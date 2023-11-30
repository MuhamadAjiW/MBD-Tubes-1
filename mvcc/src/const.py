class Status:
    ABORT = "ABORTED"
    COMMIT = "COMMITED"
    ACTIVE = "NOT COMMITED"

class Pattern:
    BEGIN = r"^begin[(]t\d+[)]$"
    WRITE = r"^w[(]t\d+,.*[)]$"
    END = r"^end$"
    COMMIT = r"^c\d+$"
    READ = r"^r[(]t\d+,.*[)]$"
    VALID = r"^^((begin)[(]t\d+[)])|(w|r)[(]t\d+,.*[)]$"
    BETWEEN = r"[(](.+?)[)]$"
