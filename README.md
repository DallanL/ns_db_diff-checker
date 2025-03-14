compares the tables of 2 netsapiens nodes and prints out important-ish differences between entries/missing entries


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

delete any entry you'd like to enter on the fly, the program will either read the .env file, OR ask you for each of these values upon running
```
HOST1=core1.yourcompany.com
USER1=readonlyDBuser
PASS1=12345
HOST2=core2.yourcompany.com
USER2=readonlyDBuser-for-core2
PASS2=54321
```

run the program and choose the table to check:
```bash
r$ python3 dbchecker.py
Select the table to compare:
  1: subscriber_config
  2: dialplan_config
  3: registrar_config
  4: callqueue_config
  5: huntgroup_config
  6: huntgroup_entry_config
Enter your choice: 
```

deactivate venv when done:
```bash
deactivate
```
