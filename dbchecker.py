import os
import mysql.connector
from dotenv import load_dotenv

# Load environment variables from .env file if present.
load_dotenv()

# Declare the database name.
DB_NAME = "SiPbxDomain"

# Define unique key fields for each table.
UNIQUE_KEY_FIELDS = {
    "subscriber_config": ["subscriber_login"],
    "dialplan_config": ["matchrule", "dialplan"],
    "registrar_config": ["aor"],
    "callqueue_config": ["queue_name", "domain"],
    "huntgroup_config": ["huntgroup_name", "huntgroup_domain"],
    "huntgroup_entry_config": ["device_aor", "huntgroup_name", "huntgroup_domain"],
    "time_frame_selections": ["domain", "user", "time_frame_name"],
    "timeframe_master": ["name", "user", "domain"],
}

# Define fields to ignore for each table.
IGNORE_FIELDS = {
    "subscriber_config": [
        "last_update",
        "presence",
        "count_session",
        "message_waiting",
        "aor_scheme",
        "vmailType",
        "moh_interval",
    ],
    "dialplan_config": [],
    "registrar_config": [
        "latency",
        "avg_latency",
        "del_latency",
        "registration_time",
        "registration_expires_time",
        "contact",
        "received_from",
        "hostname",
        "mode",
        "date_created",
        "transport",
        "from_address",
        "user_agent",
        "wan_ua",
    ],
    "callqueue_config": [],
    "huntgroup_config": [
        "last_stat_run",
    ],
    "huntgroup_entry_config": ["last_update", "session_count"],
    "time_frame_selections": [],
}


def choose_table():
    """
    Ask the user which table to run the diff on, with numeric options.
    """
    tables = {
        "1": "subscriber_config",
        "2": "dialplan_config",
        "3": "registrar_config",
        "4": "callqueue_config",
        "5": "huntgroup_config",
        "6": "huntgroup_entry_config",
        "7": "time_frame_selections",
        "8": "timeframe_master",
    }
    print("Select the table to compare:")
    for option, table in tables.items():
        print(f"  {option}: {table}")
    choice = input("Enter your choice: ").strip()
    selected_table = tables.get(choice)
    if not selected_table:
        print("Invalid choice, defaulting to 'subscriber_config'.")
        selected_table = "subscriber_config"
    return selected_table


def fetch_table_data(conn, table):
    cursor = conn.cursor(dictionary=True)
    query = f"SELECT * FROM {table}"
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    return rows


def normalize_value(value):
    """Convert None to an empty string and strip whitespace from strings."""
    if value is None:
        return ""
    return str(value).strip()


def get_unique_key(row, unique_fields):
    """
    Build a tuple key based on the given unique_fields.
    """
    return tuple(normalize_value(row.get(field, "")) for field in unique_fields)


def compare_data(valid_data, test_data, unique_fields, ignore_fields, HOST2):
    """
    Compare rows from the valid (host1) and test (host2) datasets using a unique key.
    Reports:
      - Missing rows (if a key from valid data is not found in test data)
      - Field-by-field differences (ignoring specified fields)
        Differences in test values are underlined for emphasis.
    Returns a list of difference messages and a total error count.
    """
    valid_dict = {get_unique_key(row, unique_fields): row for row in valid_data}
    test_dict = {get_unique_key(row, unique_fields): row for row in test_data}

    diffs = []
    error_count = 0

    # Check rows from valid DB (host1).
    for key, valid_row in valid_dict.items():
        if key not in test_dict:
            diffs.append(
                f"Missing in test DB {HOST2} for unique key {key}:\n  Valid row: {valid_row}"
            )
            error_count += 1
        else:
            test_row = test_dict[key]
            # Compare each field except those to ignore.
            for field in valid_row.keys():
                if field in ignore_fields:
                    continue
                valid_val = normalize_value(valid_row[field])
                test_val = normalize_value(test_row.get(field, ""))
                if valid_val != test_val:
                    diff_line = (
                        f"Difference for key {key} in field '{field}':\n"
                        f"  Valid (host1): {valid_val}\n"
                        f"  Test  (host2): {test_val}"
                    )
                    diffs.append(diff_line)
                    error_count += 1

    # Check for extra rows in test DB.
    for key, test_row in test_dict.items():
        if key not in valid_dict:
            diffs.append(
                f"Extra row in test DB {HOST2} for unique key {key}:\n  Test row: {test_row}"
            )
            error_count += 1

    return diffs, error_count


def get_env_or_prompt(var_name, prompt_msg):
    """
    Get an environment variable; if not present, prompt the user.
    """
    value = os.environ.get(var_name)
    if not value:
        value = input(prompt_msg)
    return value


def main():
    # Let the user choose the table.
    selected_table = choose_table()

    # Retrieve unique key and ignore fields based on the table.
    if selected_table not in UNIQUE_KEY_FIELDS:
        print(f"No unique key logic defined for table '{selected_table}'. Exiting.")
        return
    unique_fields = UNIQUE_KEY_FIELDS[selected_table]
    ignore_fields = IGNORE_FIELDS.get(selected_table, [])

    # Get connection credentials for host1 (valid DB) from .env or prompt if missing.
    valid_db_config = {
        "host": get_env_or_prompt("HOST1", "Enter VALID DB Host (HOST1): "),
        "user": get_env_or_prompt("USER1", "Enter VALID DB User (USER1): "),
        "password": get_env_or_prompt("PASS1", "Enter VALID DB Password (PASS1): "),
        "database": DB_NAME,
    }

    # Get connection credentials for host2 (test DB) from .env or prompt if missing.
    test_db_config = {
        "host": get_env_or_prompt("HOST2", "Enter TEST DB Host (HOST2): "),
        "user": get_env_or_prompt("USER2", "Enter TEST DB User (USER2): "),
        "password": get_env_or_prompt("PASS2", "Enter TEST DB Password (PASS2): "),
        "database": DB_NAME,
    }
    HOST2 = test_db_config.get("host")

    try:
        valid_conn = mysql.connector.connect(**valid_db_config)
        test_conn = mysql.connector.connect(**test_db_config)
    except mysql.connector.Error as err:
        print("Error connecting to databases:", err)
        return

    print(f"\nComparing table '{selected_table}' in database '{DB_NAME}'")
    print(f"Using unique fields: {unique_fields}")
    print(f"Ignoring fields: {ignore_fields}")

    valid_data = fetch_table_data(valid_conn, selected_table)
    test_data = fetch_table_data(test_conn, selected_table)

    diffs, error_count = compare_data(
        valid_data, test_data, unique_fields, ignore_fields, HOST2
    )

    if diffs:
        print("\nDifferences found:")
        for diff in diffs:
            print(diff)
    else:
        print("\nNo differences found.")

    print(f"\nTotal differences (missing/incorrect entries in test DB): {error_count}")

    valid_conn.close()
    test_conn.close()


if __name__ == "__main__":
    main()
