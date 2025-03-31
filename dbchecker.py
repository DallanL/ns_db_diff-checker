import os
import json
import mysql.connector
from dotenv import load_dotenv

# ANSI escape codes for underlining text (if needed)
UNDERLINE_START = "\033[4m"
UNDERLINE_END = "\033[0m"

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
    "timeframe_master": ["id"],
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


def compare_data(valid_data, test_data, unique_fields, ignore_fields, target_host):
    """
    Compare rows from the primary (valid) and target (test) datasets using a unique key.
    For rows that exist in both but have differences (excluding ignored fields),
    print the entire row from both hosts with only the differing fields in the test row underlined.
    Also report missing or extra rows.
    Returns a list of difference messages and a total error count.
    """
    valid_dict = {get_unique_key(row, unique_fields): row for row in valid_data}
    test_dict = {get_unique_key(row, unique_fields): row for row in test_data}

    diffs = []
    error_count = 0

    # Compare rows present in the primary DB.
    for key, valid_row in valid_dict.items():
        if key not in test_dict:
            diffs.append(
                f"Missing in {target_host} for unique key {key}:\n  Primary row: {valid_row}"
            )
            error_count += 1
        else:
            test_row = test_dict[key]
            # Determine which fields (except ignored ones) differ.
            differing_fields = []
            for field in valid_row.keys():
                if field in ignore_fields:
                    continue
                valid_val = normalize_value(valid_row[field])
                test_val = normalize_value(test_row.get(field, ""))
                if valid_val != test_val:
                    differing_fields.append(field)
            # If there are any differences, print the entire row.
            if differing_fields:
                primary_row_str = "Primary row:\t"
                test_row_str = "Test row:\t"
                # Use sorted keys for consistent ordering.
                all_fields = sorted(valid_row.keys())
                for field in all_fields:
                    val1 = normalize_value(valid_row.get(field, ""))
                    val2 = normalize_value(test_row.get(field, ""))
                    primary_row_str += f"  {field}: {val1} |"
                    if field in differing_fields:
                        test_row_str += (
                            f"  {field}: {UNDERLINE_START}{val2}{UNDERLINE_END} |"
                        )
                    else:
                        test_row_str += f"  {field}: {val2} |"
                diff_msg = (
                    f"Difference for key {key}:\n"
                    + primary_row_str
                    + "\n"
                    + test_row_str
                )
                diffs.append(diff_msg)
                error_count += 1

    # Check for extra rows in the target DB.
    for key, test_row in test_dict.items():
        if key not in valid_dict:
            diffs.append(
                f"Extra row in {target_host} for unique key {key}:\n  {target_host} row: {test_row}"
            )
            error_count += 1

    return diffs, error_count


def get_hosts_from_env():
    """
    Load a dictionary of hosts from the HOSTS environment variable (expected to be a JSON string).
    If not properly formatted, error out.
    """
    hosts_str = os.environ.get("HOSTS")
    if not hosts_str:
        print("Error: HOSTS environment variable not set. Exiting.")
        exit(1)
    try:
        hosts = json.loads(hosts_str)
    except json.JSONDecodeError as e:
        print(f"Error: HOSTS environment variable is not valid JSON. Exiting.{e}")
        exit(1)
    if not isinstance(hosts, dict):
        print("Error: HOSTS environment variable must be a JSON dictionary. Exiting.")
        exit(1)
    for host, creds in hosts.items():
        if (
            not isinstance(creds, dict)
            or "username" not in creds
            or "password" not in creds
        ):
            print(
                f"Error: Host {host} is not properly formatted. It must be a dictionary with 'username' and 'password'. Exiting."
            )
            exit(1)
    return hosts


def main():
    # Let the user choose which table to compare.
    selected_table = choose_table()

    # Retrieve unique key and ignore fields based on the table.
    if selected_table not in UNIQUE_KEY_FIELDS:
        print(f"No unique key logic defined for table '{selected_table}'. Exiting.")
        return
    unique_fields = UNIQUE_KEY_FIELDS[selected_table]
    ignore_fields = IGNORE_FIELDS.get(selected_table, [])

    # Get the dictionary of hosts from the environment variable.
    hosts = get_hosts_from_env()
    if not hosts:
        print("No hosts provided. Exiting.")
        return

    # Use the first host in the dictionary as the primary host.
    primary_host = list(hosts.keys())[0]
    primary_creds = hosts[primary_host]
    print(f"\nPrimary host (reference): {primary_host}")

    # Connect to the primary host.
    try:
        primary_conn = mysql.connector.connect(
            host=primary_host,
            user=primary_creds["username"],
            password=primary_creds["password"],
            database=DB_NAME,
        )
    except mysql.connector.Error as err:
        print(f"Error connecting to primary host {primary_host}: {err}")
        return

    # Fetch data from the primary host.
    primary_data = fetch_table_data(primary_conn, selected_table)
    print(
        f"\nComparing table '{selected_table}' in database '{DB_NAME}' on primary host: {primary_host}"
    )
    print(f"Using unique fields: {unique_fields}")
    print(f"Ignoring fields: {ignore_fields}\n")

    # Loop over each target host (all hosts except primary).
    for host, creds in hosts.items():
        if host == primary_host:
            continue
        try:
            target_conn = mysql.connector.connect(
                host=host,
                user=creds["username"],
                password=creds["password"],
                database=DB_NAME,
            )
        except mysql.connector.Error as err:
            print(f"Error connecting to host {host}: {err}")
            continue

        target_data = fetch_table_data(target_conn, selected_table)
        diffs, error_count = compare_data(
            primary_data, target_data, unique_fields, ignore_fields, host
        )
        print(f"--- Comparison with {host} ---")
        if diffs:
            for diff in diffs:
                print(diff)
        else:
            print("No differences found.")
        print(f"Total differences for {host}: {error_count}\n")
        target_conn.close()

    primary_conn.close()


if __name__ == "__main__":
    main()
