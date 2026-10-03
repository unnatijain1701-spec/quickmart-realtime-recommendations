"""db.py - one place that opens the MongoDB connection."""
import sys
from pymongo import MongoClient
import config


def get_db():
    if not config.MONGO_URI:
        sys.exit("MONGO_URI is empty. Copy .env.example to .env and fill it in (see README).")
    client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=8000)
    try:
        client.admin.command("ping")
    except Exception as e:
        sys.exit(
            "Could not reach MongoDB Atlas: " + str(e) +
            "\nCheck: 1) MONGO_URI/password in .env  2) your IP is allowed in Atlas > Network Access"
        )
    return client[config.DB_NAME]
