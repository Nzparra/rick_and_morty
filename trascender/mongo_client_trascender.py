from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

class MongoDBClient:
    def __init__(self, uri = "mongodb+srv://nzparra:Duna1016.@trascenderglobal.qkzhkak.mongodb.net/?retryWrites=true&w=majority", server_api_version='1'):
        self.uri = uri
        self.server_api = ServerApi(server_api_version)
        self.client = MongoClient(self.uri, server_api=self.server_api)
        self.db_name = 'trascenderglobal'

    def is_connected(self):
        try:
            self.client.admin.command('ping')
            return True
        except Exception as e:
            return False
    
    def create_collection(self, db_name, collection_name):
        try:
            db = self.client[db_name]
            db.create_collection(collection_name)
            print(f"La colección '{collection_name}' ha sido creada con éxito en la base de datos '{db_name}'.")
        except Exception as e:
            print(f"Error al crear la colección: {str(e)}")

    def clear_collection(self, db_name, collection_name):
        try:
            db = self.client[db_name]
            result = db[collection_name].delete_many({})
            print(f"Se eliminaron {result.deleted_count} documentos de la colección '{collection_name}' en la base de datos '{db_name}'.")
        except Exception as e:
            print(f"Error al borrar documentos de la colección: {str(e)}")

    def close_connection(self):
        self.client.close()
    
    def insert_data_from_dict(self, db_name, collection_name, data):
        try:
            db = self.client[db_name]
            collection = db[collection_name]
            result = collection.insert_many(data)
            print(f"Se han insertado {len(result.inserted_ids)} documentos en la colección '{collection_name}' en la base de datos '{db_name}'.")
        except Exception as e:
            print(f"Error al insertar datos: {str(e)}")