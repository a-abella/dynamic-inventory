# Ansible dynamic inventory boilerplate

Components for a MySQL-based Ansible dynamic inventory system

* Construct Ansible dynamic inventory json from MySQL server inventory table
* Query hostvar data for subsets of hosts
* List groups and retrieve group members
* Add hosts to MySQL inventory table

### Usage with Ansible
```
ansible-playbook -i inventory.py playbook.yml
```

### Command-line interface

```
usage: inventory.py [-h] [-l] {get,add} ...

Retrieve and insert dynamic inventory hosts

positional arguments:
  {get,add}
    get       retrieve hosts or groups
    add       write new hosts to inventory

optional arguments:
  -h, --help  show this help message and exit
  -l, --list  output entire inventory
```

