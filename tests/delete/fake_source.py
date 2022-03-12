from pushsource import Source, RpmPushItem, ModuleMdPushItem


class FakeSource(Source):
    def __init__(self):
        self.pushitems = [
            "RHSA-1111:22: new module: rhel8",
            RpmPushItem(
                name="bash-1.23-1.test8_x86_64.rpm",
                signing_key="aabbcc",
                dest=["some-yumrepo"],
            ),
            RpmPushItem(
                name="dash-1.23-1.test8_x86_64.rpm",
                signing_key="aabbcc",
                dest=["some-yumrepo"],
            ),
            RpmPushItem(
                name="crash-2.23-1.test8_x86_64.rpm",
                signing_key="aabbcc",
                dest=["other-yumrepo"],
            ),
            RpmPushItem(
                name="crash-2.23-1.test8_s390x.rpm",
                signing_key="aabbcc",
                dest=["some-other-yumrepo"],
            ),
            ModuleMdPushItem(
                name="modulemd.x86_64.txt",
                build="foo-1.0-1",
            ),
        ]

    def __iter__(self):
        for item in self.pushitems:
            yield item
