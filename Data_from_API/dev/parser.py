import pandas as pd
from uuid import uuid4

class Parser:
    def __init__(self, response, uuid_from_prefect = str(uuid4())):
        self.response = response
        self.uuid_from_prefect = uuid_from_prefect
        self.parsed_response = self.parse()

    def parse(self):
        parsed_data = []

        # Check datatype of recieved data
        if isinstance(self.response, dict):
            update_date = (self.response.get('time')).get('updatedISO')

            for currency, details in self.response.get('bpi').items():
                parsed_data.append([currency, details['rate_float'], update_date, self.uuid_from_prefect])
            
            df = pd.DataFrame(parsed_data, columns=['currency', 'details', 'update_date', 'uuid'])
        print(df)
        return df