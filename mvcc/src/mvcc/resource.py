class Resource:
    def __init__(self, name, content="Init"):
        self._name = name
        self._version = {0: [0, 0, content]}

    def add_version(self, id, rts, wts):
        self._version[id] = [rts, wts]

    def update_rts(self, id, rts):
        self._version[id][0] = rts

    def update_wts(self, id, wts):
        self._version[id][1] = wts

    def update_content(self, id, content):
        self._version[id][2] = content

    def get_version(self):
        return self._version

    def __str__(self):
        resource = f"Name: {self._name}\nVers: {self._version}\n"
        return resource
