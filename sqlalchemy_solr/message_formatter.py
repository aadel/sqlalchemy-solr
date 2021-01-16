class MessageFormatter:

    header = '*' * 24 + '/n'
    footer = '/n' + '*' * 24
    
    def format(self, message: str):
        return self.header + message + self.footer