import mysql.connector
import pytest
from dotenv import load_dotenv
import os
import datetime

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


def test_students_table_exists(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("SHOW TABLES LIKE 'students'")
    result = cursor.fetchone()
    assert result is not None, "Table 'students' does not exist."


def test_rooms_table_exists(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("SHOW TABLES LIKE 'rooms'")
    result = cursor.fetchone()
    assert result is not None, "Table 'rooms' does not exist."


def test_students_table_populated(db_connection):
    cursor = db_connection.cursor(dictionary=True)
    cursor.execute("SELECT * FROM students WHERE id = 0")
    result = cursor.fetchone()
    expected_data = {
        "id": 0,
        "name": "Peggy Ryan",
        "birthdate": datetime.datetime(2011, 8, 22, 0, 0),
        "sex": "M",
        "room": 473,
    }
    assert result == expected_data, f"Expected {expected_data}, but got {result}"


def test_rooms_table_populated(db_connection):
    cursor = db_connection.cursor(dictionary=True)
    cursor.execute("SELECT * FROM rooms WHERE id = 0")
    result = cursor.fetchone()
    expected_data = {"id": 0, "name": "Room #0"}
    assert result == expected_data, f"Expected {expected_data}, but got {result}"
