import StringIO

class StringIO(StringIO.StringIO):
    def __init__(self, *args, **kws):
        super(StringIO, self).__init__(*args, **kws)
        self.indentlvl = 0

    def indent(self, by=4):
        self.indentlvl += by

    def dedent(self, by=4):
        self.indentlvl -= by

    def write(self, *args, **kws):
        super(StringIO, self).write(self.indentlvl * ' ')
        super(StringIO, self).write(*args, **kws)

    def writeln(self, *args, **kws):
        self.write(*args, **kws)
        super(StringIO, self).write('\n')
