import json
import sys
from decimal import Decimal
from dicttoxml import dicttoxml
from db import DatabaseInitializer, TableManager, DataPopulator, DataQueries
from db_config import config, config_init

OUTPUT_DIR = "output"
JSON_FORMAT = "json"
XML_FORMAT = "xml"


def decimal_serializer(obj):
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError("Type not serializable")


def initialize_database():
    db_init = DatabaseInitializer(config_init)
    db_init.create_db()


def create_tables():
    table_manager = TableManager(config)
    table_manager.create_tables()


def populate_database(filepath_students, filepath_rooms):
    data_populator = DataPopulator(config)
    data_populator.populate_db(filepath_students, filepath_rooms)


def fetch_queries():
    data_queries = DataQueries(config)
    return [
        data_queries.get_rooms_with_students_number(),
        data_queries.get_five_rooms_with_youngest_students(),
        data_queries.get_five_rooms_with_biggest_age_difference(),
        data_queries.get_rooms_with_different_student_sexes(),
    ]


def save_results_as_json(queries):
    for counter, query in enumerate(queries, start=1):
        json_results = json.dumps(query, indent=4, default=decimal_serializer)
        output_file_name = f"{OUTPUT_DIR}/result_{counter}.json"
        with open(output_file_name, "w") as json_file:
            json_file.write(json_results)


def save_results_as_xml(queries):
    for counter, query in enumerate(queries, start=1):
        xml_results = dicttoxml(
            query, custom_root="rooms", attr_type=False, return_bytes=False
        )
        output_file_name = f"{OUTPUT_DIR}/result_{counter}.xml"
        with open(output_file_name, "w") as xml_file:
            xml_file.write(xml_results)


def main():
    try:
        if len(sys.argv) < 4:
            raise ValueError("Insufficient arguments provided")

        filepath_students, filepath_rooms, output_format = (
            sys.argv[1],
            sys.argv[2],
            sys.argv[3],
        )

        initialize_database()
        create_tables()
        populate_database(filepath_students, filepath_rooms)

        queries = fetch_queries()

        if output_format == JSON_FORMAT:
            save_results_as_json(queries)
        elif output_format == XML_FORMAT:
            save_results_as_xml(queries)
        else:
            print("Non-supported format")
    except ValueError as ve:
        print(f"Argument error: {ve}")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
