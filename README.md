compares the tables of a list of netsapiens nodes and prints out important-ish differences between entries/missing entries


### INSTALL

clone repo:
```bash
git clone https://github.com/DallanL/ns_db_diff-checker.git
```

setup venv and install requirements:
```bash
cd ns_db_diff-checker
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

update env file:
```bash
cp env-evample .env
vim .env
```

#### .env Configuration
The new version uses a JSON-formatted dictionary in the HOSTS environment variable. This dictionary should contain the host URLs as keys and a dictionary with "username" and "password" as values. For example:
```
HOSTS={"core1.yourcompany.com": {"username": "readonlyDBuser", "password": "12345"}, "core2.yourcompany.com": {"username": "readonlyDBuser-for-core2", "password": "54321"}}
```

### Usage
run the program and choose the table to check:
```bash
python3 dbchecker.py
```

you will be prompted to select which table to compare:
```
Select the table to compare:
  1: subscriber_config
  2: dialplan_config
  3: registrar_config
  4: callqueue_config
  5: huntgroup_config
  6: huntgroup_entry_config
  7: time_frame_selections
  8: timeframe_master
Enter your choice: 
```

deactivate venv when done:
```bash
deactivate
```
