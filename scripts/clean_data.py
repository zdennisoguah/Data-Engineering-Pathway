import pandas as pd
import logging
import os
# 1. Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

#2. File Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_FILE = os.path.join(BASE_DIR, "data", "raw_trips.csv")
OUTPUT_FILE = os.path.join(BASE_DIR, "data", "cleaned_trips.json")
def main():
    try:
        logging.info(f"Reading raw trips data from {RAW_FILE}")


        # 3. Read Data
        df = pd.read_csv(RAW_FILE)

        logging.info(f"Rows before cleaning: {len(df)}")


        # 4. Clean Column Names
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(" ", "_")
        )


        # 5. Drop Fully Empty Rows
        before_drop = len(df)
        df = df.dropna(how="all")
        dropped_rows = before_drop - len(df)

        logging.info(f"Dropped {dropped_rows} fully empty rows")

        # 6. Handle Missing Values
        # Convert numeric columns
        df["fare_amount"] = pd.to_numeric(df["fare_amount"], errors="coerce")
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")

        # Fill missing values
        df["rating"] = df["rating"].fillna(0.0)
        df["payment_type"] = df["payment_type"].fillna("unknown")

        avg_fare = df["fare_amount"].mean()
        df["fare_amount"] = df["fare_amount"].fillna(avg_fare)



        # 7. Data Quality Flag

        df["data_quality"] = df.apply(
            lambda row: "incomplete"
            if pd.isna(row["driver_name"]) or pd.isna(row["driver_id"])
            else "ok",
            axis=1
        )

         # 8. Summary Log
        incomplete_count = (df["data_quality"] == "incomplete").sum()
        logging.info(f"Rows after cleaning: {len(df)}")
        logging.info(f"Incomplete records flagged: {incomplete_count}")

        # 9. Write to JSON
        df.to_json(OUTPUT_FILE, orient="records", indent=4)
        logging.info(f"Cleaned trips written to {OUTPUT_FILE}")

    except FileNotFoundError:
        logging.error(f"File not found: {RAW_FILE}")

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")



# 10. Entry Point

if __name__ == "__main__":
    main()
