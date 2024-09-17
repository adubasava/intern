import mysql.connector
import pytest
from dotenv import load_dotenv
import os
from decimal import Decimal

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


def test_rooms_with_students(db_connection):
    cursor = db_connection.cursor(dictionary=True)
    query = """
        SELECT rooms.id, rooms.name, COUNT(students.id) AS student_count
        FROM rooms
        LEFT JOIN students ON rooms.id = students.room
        GROUP BY rooms.id, rooms.name
        LIMIT 1;
        """
    cursor.execute(query)
    result = cursor.fetchone()
    expected_data = {"id": 0, "name": "Room #0", "student_count": 9}
    assert result == expected_data, f"Expected {expected_data}, but got {result}"


def test_rooms_with_youngest_students(db_connection):
    cursor = db_connection.cursor(dictionary=True)
    query = """
        SELECT rooms.name AS room_name, AVG(TIMESTAMPDIFF(YEAR, students.birthdate, CURDATE())) AS avg_age 
        FROM students
        JOIN rooms ON students.room = rooms.id 
        GROUP BY rooms.name 
        ORDER BY avg_age ASC 
        LIMIT 1;    
        """
    cursor.execute(query)
    result = cursor.fetchone()
    expected_data = {"room_name": "Room #661", "avg_age": Decimal("11.0000")}
    assert result == expected_data, f"Expected {expected_data}, but got {result}"


def test_rooms_with_biggest_age_difference(db_connection):
    cursor = db_connection.cursor(dictionary=True)
    query = """
        SELECT rooms.name AS room_name, 
        MAX(TIMESTAMPDIFF(YEAR, students.birthdate, CURDATE())) - MIN(TIMESTAMPDIFF(YEAR, students.birthdate, CURDATE())) AS age_difference
        FROM students
        JOIN rooms ON students.room = rooms.id
        GROUP BY rooms.name
        ORDER BY age_difference DESC
        LIMIT 1;
        """
    cursor.execute(query)
    result = cursor.fetchone()
    expected_data = {"room_name": "Room #213", "age_difference": 116}
    assert result == expected_data, f"Expected {expected_data}, but got {result}"


def test_rooms_with_different_student_sexes(db_connection):
    cursor = db_connection.cursor(dictionary=True)
    query = """
        SELECT rooms.name AS room_name
        FROM students
        JOIN rooms ON students.room = rooms.id
        GROUP BY rooms.name
        HAVING COUNT(DISTINCT students.sex) = 2;
        """
    cursor.execute(query)
    result = cursor.fetchall()
    expected_data = 990
    assert len(result) == expected_data, f"Expected {expected_data}, but got {result}"
