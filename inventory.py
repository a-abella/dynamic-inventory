#!/usr/bin/env python3

import argparse
import ipaddress
import json
import pymysql
import socket
import sys

'''
usage: inventory.py [-h] [-l] {get,add} ...

Retrieve and insert dynamic inventory hosts

positional arguments:
  {get,add}
    get       retrieve hosts or groups
    add       write new hosts to inventory

optional arguments:
  -h, --help  show this help message and exit
  -l, --list  output entire inventory

=====

* Construct Ansible dynamic inventory json from MySQL server inventory table
* Query hostvar data for subsets of hosts
* List groups and retrieve group members
* Add hosts to MySQL inventory table

Usage with Ansible:
    ansible-playbook -i inventory.py playbook.yml
'''


class Host:
    def __init__(self, id, fqdn, enabled, features, ipaddr, label, groups, upd):
        self.id = id
        self.fqdn = fqdn
        self.upd = upd

        # convert mysql int-based boolean to real boolean
        self.enabled = True if enabled else False

        # create objects from parsable fields
        self.ipaddr = ipaddress.ip_address(ipaddr)

        # return empty data types if Null in table
        self.groups = groups.split(',') if groups else []
        self.features = features.split(',') if features else []
        self.label = label if label else ''


def parse_args():
    parser = argparse.ArgumentParser(
        description='Retrieve and insert dynamic inventory hosts'
    )
    subparsers = parser.add_subparsers(
        dest='subparser'
    )

    # Ansible's spec requires dynamic inventories implement a top-level `--list`
    # argument that outputs the entire inventory
    parser.add_argument(
        '-l', '--list', help='output entire inventory', action='store_true'
    )

    get = subparsers.add_parser(
        'get', help='retrieve hosts or groups'
    )
    get_subparsers = get.add_subparsers(
        dest='get_subparser'
    )
    get_host = get_subparsers.add_parser(
        'host', help='retrieve hosts by hostname'
    )
    get_host.add_argument(
        'name', metavar='NAME', help='return hosts matching NAME, performs startswith matching; special case `all` returns all hosts', type=str, nargs=1
    )
    get_group = get_subparsers.add_parser(
        'group', help='list valid group names and retrieve hosts belonging to groups'
    )
    group_mut_ex = get_group.add_mutually_exclusive_group(required=True)
    group_mut_ex.add_argument(
        'name', metavar='NAME', help='name of group for which to list contents, performs exact matching', type=str, nargs='?'
    )
    group_mut_ex.add_argument(
        '-l', '--list', help='list all group names', action='store_true'
    )

    add = subparsers.add_parser(
        'add', help='write new hosts to inventory'
    )
    add.add_argument(
        'name', metavar='NAME', help='fully-qualified domain name (FQDN) of host', type=str, nargs=1
    )
    add.add_argument(
        '-i', '--ipaddr', help='ip address of host; if not provided will attempt to resolve ip from FQDN', type=str, required=False
    )
    add.add_argument(
        '-g', '--groups', help='comma-delimited list of groups the host will belong to, ex: `-g group1,group2,group3`', required=False
    )
    add.add_argument(
        '-f', '--features', help='comma-delimited list of feature flags to enable on the host, ex: `-f feature1,feature2`', required=False
    )
    add.add_argument(
        '-l', '--label', help='cosmetic label describing the host', type=str, required=False
    )
    add.add_argument(
        '-d', '--disabled', help='add the host in a disabled state; new hosts default to enabled', action='store_true'
    )

    try:
        args = parser.parse_args()
        if not args.subparser and not args.list:
            parser.error('requires one of {get,add}, or -l/--list')
        if args.subparser == 'get' and not args.get_subparser:
            get.error('required one of {host,group}')
        return args
    except argparse.ArgumentTypeError as err:
        parser.error(err.args[1])


def connect_db(
    host='127.0.0.1',
    port=3306,
    user='root',
    passwd='password',
    db='db'
):
    db = pymysql.connect(
        host=host,
        port=port,
        user=user,
        passwd=passwd,
        db=db,
        connect_timeout=10
    )
    return db


def read_query(db, query):
    cur = db.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    output = cur.fetchall()
    cur.close()
    return output


def write_query(db, query):
    cur = db.cursor()
    try:
        cur.execute(query)
        db.commit()
        cur.close()
    except Exception as err:
        print('ERROR:', err.args[1])
        cur.close()
        db.close()
        sys.exit(1)


def get_hosts(
    where='1 = 1',
    select='`id`, `fqdn`, `enabled`, `features`, `ipaddr`, `label`, `groups`, `upd`'
):
    db = connect_db()
    query = 'SELECT {select} FROM `server_inventory` WHERE {where} ;'.format(
        select=select, where=where)
    host_list = read_query(db, query)
    db.close()
    return host_list


def process_hosts(host_list):
    hosts = []
    for host in host_list:
        hosts.append(
            Host(
                id=host['id'],
                fqdn=host['fqdn'],
                enabled=host['enabled'],
                features=host['features'],
                ipaddr=host['ipaddr'],
                label=host['label'],
                groups=host['groups'],
                upd=host['upd']
            )
        )
    return hosts


def build_hostvars(hosts):
    hostvars = {}
    for host in hosts:
        host_dict = host.__dict__.copy()
        del host_dict['fqdn']
        host_dict['upd'] = host_dict['upd'].strftime('%Y-%d-%m %H:%M:%S')
        host_dict['ipaddr'] = host_dict['ipaddr'].exploded
        hostvars[host.fqdn] = host_dict
    return hostvars


def build_groups(hosts):
    groups = {}
    for host in hosts:
        for group in host.groups:
            if group not in groups:
                groups[group] = {'hosts': []}
            groups[group]['hosts'].append(host.fqdn)
    return groups


def build_ansible_inventory(groups, hostvars):
    inventory = groups.copy()
    inventory['_meta'] = {
        'hostvars': hostvars
    }
    return inventory


def add_host(name, ipaddr=None, groups=None, features=None, label=None, disabled=False):
    db = connect_db()
    fields = ['fqdn', 'ipaddr']
    values = [name]
    if ipaddr:
        values.append(ipaddr)
    else:
        try:
            values.append(
                socket.gethostbyname(name)
            )
        except socket.gaierror as err:
            print('ERROR:', err.args[1])
            db.close()
            sys.exit(1)
    if groups:
        fields.append('groups')
        values.append(groups)
    if features:
        fields.append('features')
        values.append(features)
    if label:
        fields.append('label')
        values.append(label)
    if disabled:
        fields.append('enabled')
        values.append(0)

    fields = ','.join(['`{}`'.format(x) for x in fields])
    values = ','.join(["'{}'".format(x) if isinstance(
        x, str) else str(x) for x in values])

    query = 'INSERT INTO `server_inventory` ({fields}) VALUES ({values});'.format(
        fields=fields, values=values)
    write_query(db, query)
    db.close()


def dump(data):
    if isinstance(data, dict):
        output = json.dumps(data)
        print(output)
    elif isinstance(data, (tuple, list, type({}.keys()))):
        output = sorted(data)
        print(*output, sep='\n')


def main(args):
    if args.subparser and args.subparser == 'add':
        add_host(args.name[0], args.ipaddr, args.groups,
                 args.features, args.label, args.disabled)
    elif not args.subparser or args.subparser == 'get':
        hosts = process_hosts(get_hosts())
        groups = build_groups(hosts)
        hostvars = build_hostvars(hosts)
        if args.subparser:
            if args.get_subparser == 'host':
                if args.name[0] != 'all':
                    host_subset = [
                        host.fqdn for host in hosts if host.fqdn.startswith(args.name[0])]
                    hostvar_subset = {}
                    for host in host_subset:
                        hostvar_subset[host] = hostvars[host]
                    dump(hostvar_subset)
                else:
                    dump(hostvars)
            elif args.get_subparser == 'group':
                if args.list:
                    dump(groups.keys())
                else:
                    try:
                        group_hosts = groups[args.name]['hosts']
                    except KeyError:
                        print('No group matching {}'.format(args.name))
                        sys.exit(1)
                    dump(group_hosts)
        elif args.list:
            dump(build_ansible_inventory(groups, hostvars))


if __name__ == '__main__':
    args = parse_args()
    main(args)
