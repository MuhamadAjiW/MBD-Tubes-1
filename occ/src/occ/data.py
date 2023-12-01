# class Data:
#     def __init__(self, name, value):
#         self.name = name
#         self.value = value

#     def get_value(self):
#         return self.value

#     def set_value(self, new_value):
#         self.value = new_value

class Data:
    def __init__(self, name, value):
        self.name = name
        self.initial_value = value
        self.current_value = value
        self.committed_value = None

    def get_value(self):
        return self.current_value

    def set_value(self, new_value):
        self.current_value = new_value

    def set_committed_value(self):
        self.committed_value = self.current_value

    def reset_value(self):
        self.current_value = self.initial_value
