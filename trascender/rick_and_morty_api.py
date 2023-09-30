import requests

class RickAndMortyAPI:
    def __init__(self, base_url="https://rickandmortyapi.com/api/"):
        self.base_url = base_url

    def get_data(self,collection):
        url = f"{self.base_url}{collection}"
        all_results = []

        while url:
            response = requests.get(url)
            
            if response.status_code == 200:
                data = response.json()
                results = data.get("results", [])
                all_results.extend(results)
                url = data.get("info", {}).get("next")
            else:
                raise Exception(f"Error al realizar la solicitud. Código de estado: {response.status_code}")
        return all_results