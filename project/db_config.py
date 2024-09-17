from dotenv import load_dotenv
import os

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PSWD = os.getenv("DB_PSWD")

config_init = {
    "host": "localhost",
    "user": DB_USER,
    "password": DB_PSWD,
}

config = {
    "host": "localhost",
    "user": DB_USER,
    "password": DB_PSWD,
    "database": "students_rooms",
}
