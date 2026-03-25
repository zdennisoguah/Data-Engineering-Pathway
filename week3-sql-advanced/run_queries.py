import duckdb
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "data", "trips.csv")
SQL_FILE = os.path.join(BASE_DIR, "sql", "analysis.sql")

def main():
    try:
        logging.info("Connecting to DuckDB in-memory database")
        conn = duckdb.connect()

        logging.info(f"Loading trips data from {DATA_FILE}")
        conn.execute(f"""
            CREATE TABLE trips AS
            SELECT * FROM read_csv_auto('{DATA_FILE}')
        """)

        row_count = conn.execute("SELECT COUNT(*) FROM trips").fetchone()[0]
        logging.info(f"Loaded {row_count} rows into trips table")

        logging.info(f"Reading queries from {SQL_FILE}")
        with open(SQL_FILE, "r") as f:
            sql_content = f.read()

        # Split queries by -- Q separator
        queries = [q.strip() for q in sql_content.split("-- Q") if q.strip()]

        for i, query in enumerate(queries, 1):
            # Extract query title from first line
            lines = query.strip().splitlines()
            title = lines[0].strip()
            sql = "\n".join(lines[1:]).strip()

            logging.info(f"Running Q{i}: {title}")
            result = conn.execute(sql).df()
            print(f"\n--- Q{i}: {title} ---")
            print(result.to_string(index=False))
            print()

        conn.close()
        logging.info("All queries completed successfully")

    except FileNotFoundError as e:
        logging.error(f"File not found: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()