from azure.storage.filedatalake import DataLakeServiceClient

STORAGE_ACCOUNT = "delakehousezdennis"
STORAGE_KEY      = "7WOg7asDRnv+nyjfZM3Qe8QKltM7x65vuL+Vytd8mZ8nh39C0XDUGjSu+lCGSg/aN0t4RUR9JTkY+AStwRqZ7w=="
CONTAINER        = "lakehouse"
LOCAL_FILE       = "/tmp/new_trips_2024_02_26.csv"
ADLS_PATH        = "bronze/landing/new_trips_2024_02_26.csv"

service_client = DataLakeServiceClient(
    account_url=f"https://{STORAGE_ACCOUNT}.dfs.core.windows.net",
    credential=STORAGE_KEY
)

fs_client   = service_client.get_file_system_client(CONTAINER)
file_client = fs_client.get_file_client(ADLS_PATH)

with open(LOCAL_FILE, "rb") as f:
    data = f.read()
    file_client.upload_data(data, overwrite=True)

print(f"Uploaded : {LOCAL_FILE}")
print(f"ADLS path: abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{ADLS_PATH}")
print(f"Status   : New data sitting in Bronze landing zone — ready for pipeline")