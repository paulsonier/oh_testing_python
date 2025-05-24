import csv
import sqlite3
import argparse
import os

def import_csv_to_db(csv_filepath, db_filepath, table_name):
    """
    Imports data from a CSV file into a SQLite database table.

    Args:
        csv_filepath (str): Path to the input CSV file.
        db_filepath (str): Path to the SQLite database file.
        table_name (str): Name of the table to create/insert data into.
    """
    if not os.path.exists(csv_filepath):
        print(f"Error: CSV file not found at '{csv_filepath}'")
        return

    conn = None
    try:
        conn = sqlite3.connect(db_filepath)
        cursor = conn.cursor()

        with open(csv_filepath, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)  # Get header row

            # Sanitize headers for SQL column names (replace spaces with underscores, etc.)
            sanitized_headers = [h.replace(' ', '_').replace('.', '').replace('-', '_').lower() for h in headers]

            # Create table schema
            columns_with_types = [f"{header} TEXT" for header in sanitized_headers] # All columns as TEXT for simplicity
            create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns_with_types)})"
            cursor.execute(create_table_sql)
            print(f"Table '{table_name}' ensured in database '{db_filepath}'")

            # Prepare insert statement
            placeholders = ', '.join(['?' for _ in sanitized_headers])
            insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"

            # Insert data row by row
            for i, row in enumerate(reader):
                if len(row) != len(sanitized_headers):
                    print(f"Warning: Skipping row {i+2} due to column count mismatch. Expected {len(sanitized_headers)}, got {len(row)}: {row}")
                    continue
                cursor.execute(insert_sql, row)
            
            conn.commit()
            print(f"Successfully imported data from '{csv_filepath}' to table '{table_name}' in '{db_filepath}'")

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import CSV data into a SQLite database.")
    parser.add_argument("csv_file", help="Path to the input CSV file.")
    parser.add_argument("db_file", help="Path to the SQLite database file (will be created if it doesn't exist).")
    parser.add_argument("table_name", help="Name of the table to import data into.")

    args = parser.parse_args()

    import_csv_to_db(args.csv_file, args.db_file, args.table_name)