from pymongo import MongoClient
from pymongo.server_api import ServerApi
import os
from dotenv import load_dotenv
from functions import update_user_history

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")

client = MongoClient(MONGO_URI, server_api=ServerApi('1'))
db = client[MONGO_DB]
users = db["users"]
user_list = [u["_id"] for u in users.find({}, {"_id": 1})]

for i in user_list:
    update_user_history(i)