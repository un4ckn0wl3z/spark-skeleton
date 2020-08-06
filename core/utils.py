class Util:

    @staticmethod
    def getShowString(df, n=20, truncate=True, vertical=False):
        if isinstance(truncate, bool) and truncate:
            return (df._jdf.showString(n, 20, vertical))
        else:
            return (df._jdf.showString(n, int(truncate), vertical))