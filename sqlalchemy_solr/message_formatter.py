class MessageFormatter:

    header = "\n" + "*" * 24 + "\n"
    footer = "\n" + "*" * 24

    def format(self, *args):
        return self.header + "\n".join(args) + self.footer
