from occ.manager import Manager

def main():
    test_case_file = 'C:\\Kuliah\\Semester 5\\MBD\\IF3140-MBD-Tubes1\\occ\\tc\\tc3.txt'
    manager = Manager(test_case_file)
    manager.run()

if __name__ == "__main__":
    main()
