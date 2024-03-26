import delta_sharing
import os
from delta_sharing import SharingClient

# Get the configuration file path
profile_file = os.path.join(os.path.dirname(__file__), "config.share")
client = SharingClient(profile_file)

downloads_folder = os.path.join(os.path.expanduser('~'), 'Downloads')

# List all Shared tables
print("\n---------------------------  All Available Tables ---------------------------")
for obj in client.list_all_tables():
    print(obj)

table_url = profile_file + "#test_mav_delta_share.test_schema.test_table"
table_url1 = profile_file + "#test_mav_delta_share.test_schema.test_table1"

# Fetch data [.load_as_pandas(table_url, limit = X)]
print("\n\n------------ Loading TABLE 1 ------------\n")
df_1 = delta_sharing.load_as_pandas(table_url)
print(df_1, "\n")

print("\n\n------------ Loading TABLE 2 ------------\n")
df_2 = delta_sharing.load_as_pandas(table_url1)
print(df_2, "\n")

# Save Files as CSV to Local Directory
print("\n---------------------------  Save into CSVs ---------------------------")
try:
    # Specify the file path(s)
    file_path_1 = os.path.join(downloads_folder, 'tbl_1.csv')
    file_path_2 = os.path.join(downloads_folder, 'tbl_2.csv')

    df_1.to_csv(file_path_1, index=False)
    print("Successfully Saved TABLE 1")

    df_2.to_csv(file_path_2, index=False)
    print("Successfully Saved TABLE 2")
except:
    print("Failure to Save CSV")
