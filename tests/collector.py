class FakeCollector(object):
    """Fake backend for PushCollector.

    Tests can access the attributes on this collector to see
    which push items & files were created during a task.
    """

    def __init__(self):
        self.items = []
        self.file_content = {}

    def update_push_items(self, items):
        self.items.extend(items)

    def attach_file(self, filename, content):
        self.file_content[filename] = content

    def append_file(self, filename, content):
        self.file_content[filename] = self.file_content.get(filename, b"") + content
