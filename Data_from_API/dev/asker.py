import requests

class Asker:
    def __init__(self, url = "https://api.coindesk.com/v1/bpi/currentprice.json"):
        self.url = url
        #get the response
        self.json_response = self.ask()

    def ask(self):
        response = requests.get(self.url)
        # Check that the server response successfully
        response.raise_for_status()
        # Print the data demo with nice format
        return response.json()
