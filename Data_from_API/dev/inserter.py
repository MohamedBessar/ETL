class Inserter:
    def __init__(self, engine, df, target = 'currency'):
        self.engine = engine
        self.df = df
        self.target = target
        self.insert()

    def insert(self):
        with self.engine.connect() as cnn:
            # Truncate the table from old data if exists
            self.df.to_sql(self.target,con=cnn, if_exists='replace', index=False)
            