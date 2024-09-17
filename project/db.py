from mysql.connector import connect, Error
from db_config import config
import json


class DatabaseManager:
    def __init__(self, config):
        self.config = config

    def execute_query(self, query, params=None, fetch=False, dictionary=False):
        try:
            with connect(**self.config) as connection:
                with connection.cursor(dictionary=dictionary) as cursor:
                    cursor.execute(query, params)
                    if fetch:
                        return cursor.fetchall()
                connection.commit()
        except Error as e:
            print(e)


class DatabaseInitializer(DatabaseManager):
    def __init__(self, config_init):
        super().__init__(config_init)
        self.config = config_init

    def create_db(self):
        create_db_query = "CREATE DATABASE IF NOT EXISTS students_rooms"
        self.execute_query(create_db_query)


class Database(DatabaseManager):
    def __init__(self, config):
        super().__init__(config)
        self.config = config
    # For testing purposes
    def show_db(self):
        show_db_query = "SHOW DATABASES"
        databases = self.execute_query(show_db_query, fetch=True)
        for db in databases:
            print(db)
    # For testing purposes
    def drop_db(self):
        drop_db_query = "DROP DATABASE students_rooms"
        self.execute_query(drop_db_query)


class TableManager(DatabaseManager):
    create_rooms_table_query = """
    CREATE TABLE IF NOT EXISTS rooms (
    id INT PRIMARY KEY,
    name VARCHAR(20) NOT NULL
    )
    """

    create_students_table_query = """
    CREATE TABLE IF NOT EXISTS students (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    birthdate DATETIME NOT NULL,
    sex ENUM('F', 'M') NOT NULL,
    room INT NOT NULL,
    FOREIGN KEY (room) REFERENCES rooms(id)
    )
    """

    def create_tables(self):
        self.execute_query(self.create_rooms_table_query)
        self.execute_query(self.create_students_table_query)


class DataPopulator(DatabaseManager):
    def populate_db(self, path_students, path_rooms):
        with open(path_students) as file:
            students_data = json.load(file)

        with open(path_rooms) as file:
            rooms_data = json.load(file)

        try:
            with connect(**self.config) as connection:
                with connection.cursor() as cursor:
                    for room in rooms_data:
                        cursor.execute(
                            "INSERT IGNORE INTO rooms (id, name) VALUES (%s, %s)",
                            (room["id"], room["name"]),
                        )
                    for student in students_data:
                        cursor.execute(
                            "INSERT IGNORE INTO students (id, name, birthdate, sex, room) VALUES (%s, %s, %s, %s, %s)",
                            (
                                student["id"],
                                student["name"],
                                student["birthday"],
                                student["sex"],
                                student["room"],
                            ),
                        )
                    connection.commit()
        except Error as e:
            print(e)

# For testing purposes
class DataDisplay(DatabaseManager):
    def display(self):
        display_query = "SELECT * FROM students LIMIT 20"
        results = self.execute_query(display_query, fetch=True)
        for result in results:
            print(result)


class DataQueries(DatabaseManager):
    def __init__(self, config):
        super().__init__(config)
        self.create_indexes()

    def create_indexes(self):
        index_queries = [
            ("idx_students_room", "CREATE INDEX idx_students_room ON students(room);"),
            (
                "idx_students_room_birthdate",
                "CREATE INDEX idx_students_room_birthdate ON students(room, birthdate);",
            ),
            (
                "idx_students_room_sex",
                "CREATE INDEX idx_students_room_sex ON students(room, sex);",
            ),
        ]

        for index_name, query in index_queries:
            if not self.index_exists(index_name):
                self.execute_query(query, fetch=False)

    def index_exists(self, index_name):
        query = """
            SELECT COUNT(*)
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = 'students_rooms' AND INDEX_NAME = %s;
            """
        result = self.execute_query(
            query, params=(index_name,), fetch=True, dictionary=True
        )
        return result[0]["COUNT(*)"] > 0

    def get_rooms_with_students_number(self):
        query = """
            SELECT room.id, room.name, COUNT(students.id) AS student_count
            FROM rooms
            LEFT JOIN students ON room.id = students.room
            GROUP BY room.id, room.name;
        """
        return self.execute_query(query, fetch=True, dictionary=True)

    def get_five_rooms_with_youngest_students(self):
        query = """
            SELECT room.name AS room_name, AVG(TIMESTAMPDIFF(YEAR, students.birthdate, CURDATE())) AS avg_age 
            FROM students
            JOIN rooms ON students.room = room.id 
            GROUP BY room.name 
            ORDER BY avg_age ASC            
            LIMIT 5;
        """
        return self.execute_query(query, fetch=True, dictionary=True)

    def get_five_rooms_with_biggest_age_difference(self):
        query = """
            SELECT room.name AS room_name, 
            MAX(TIMESTAMPDIFF(YEAR, students.birthdate, CURDATE())) - MIN(TIMESTAMPDIFF(YEAR, students.birthdate, CURDATE())) AS age_difference
            FROM students
            JOIN rooms ON students.room = room.id
            GROUP BY room.name
            ORDER BY age_difference DESC            
            LIMIT 5;
        """
        return self.execute_query(query, fetch=True, dictionary=True)

    def get_rooms_with_different_student_sexes(self):
        query = """
            SELECT room.name AS room_name
            FROM students
            JOIN rooms ON students.room = room.id
            GROUP BY room.name
            HAVING COUNT(DISTINCT students.sex) = 2;
        """
        return self.execute_query(query, fetch=True, dictionary=True)


# For testing purposes
manage_db = Database(config)
# manage_db.drop_db()
