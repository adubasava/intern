import mysql.connector
import pytest
from dotenv import load_dotenv
import os

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PSWD = os.getenv("DB_PSWD")

config = {
    "host": "localhost",
    "user": DB_USER,
    "password": DB_PSWD,
    "database": "students_rooms",
}


@pytest.fixture(scope="module")
def db_connection():
    connection = mysql.connector.connect(**config)
    yield connection
    connection.close()


def test_indexes_exist(db_connection):
    cursor = db_connection.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT INDEX_NAME
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = 'students_rooms' AND TABLE_NAME = 'students';
        """
    )
    indexes = cursor.fetchall()
    index_names = [index["INDEX_NAME"] for index in indexes]

    assert "idx_students_room" in index_names
    assert "idx_students_room_birthdate" in index_names
    assert "idx_students_room_sex" in index_names
