from trascender.mongo_client_trascender import MongoDBClient
from trascender.rick_and_morty_api import RickAndMortyAPI

# Función para obtener y cargar datos en una colección
def get_and_load_data(collection_name, data_func,mongodb_client):
    data = data_func(collection_name)
    mongodb_client.clear_collection(mongodb_client.db_name, collection_name)
    mongodb_client.create_collection(mongodb_client.db_name, collection_name)
    mongodb_client.insert_data_from_dict(mongodb_client.db_name, collection_name, data)

def main():
    mongodb_client = MongoDBClient()
    
    if mongodb_client.is_connected():
        print("Pinged your deployment. You successfully connected to MongoDB!")
    else:
        print("Failed to connect to MongoDB.")
        return

    try:
        rick_and_morty = RickAndMortyAPI()

        # Obtener y cargar datos de personajes
        characters_collection_name = 'character'
        get_and_load_data(characters_collection_name, rick_and_morty.get_data,mongodb_client)

        # Obtener y cargar datos de ubicaciones
        locations_collection_name = 'location'
        get_and_load_data(locations_collection_name, rick_and_morty.get_data,mongodb_client)

        # Obtener y cargar datos de episodios
        episodes_collection_name = 'episode'
        get_and_load_data(episodes_collection_name, rick_and_morty.get_data,mongodb_client)
    
    except Exception as e:
        print(f"Error: {e}")

    mongodb_client.close_connection()

if __name__ == "__main__":
    main()