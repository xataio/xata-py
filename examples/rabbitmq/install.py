#!/usr/bin/env python

import os
import logging

from xata.client import XataClient
from dotenv import load_dotenv

logging.basicConfig(format='%(asctime)s [%(process)d] %(levelname)s: %(message)s', level=logging.INFO)
load_dotenv()

XATA_DATABASE_NAME = os.environ.get('XATA_DB_NAME')
assert XATA_DATABASE_NAME, "env.XATA_DB_NAME not set."

xata = XataClient(db_name=XATA_DATABASE_NAME)

logging.info("checking credentials ..")
assert xata.users().getUser().status_code == 200, "Unable to connect to Xata. Please check credentials"

logging.info("creating database '%s' .." % XATA_DATABASE_NAME)
r = xata.databases().createDatabase(
    xata.get_config()["workspaceId"], 
    XATA_DATABASE_NAME, 
    {"region": xata.get_config()["region"]},
)
assert r.status_code == 201, "Unable to create database '%s': %s" % (XATA_DATABASE_NAME, r.json())

logging.info("creating table 'companies' ..")
r = xata.table().createTable("companies")
assert r.status_code == 201, "Unable to create table 'companies'"

logging.info("creating table 'prices' ..")
r = xata.table().createTable("prices")
assert r.status_code == 201, "Unable to create table 'prices'"

logging.info("setting table schema of table 'companies' ..")
r = xata.table().setTableSchema('companies', {"columns": [
    {
        "name": "name", 
        "type": "string"
    },
    {
        "name": "address", 
        "type": "string"
    },
    {
        "name": "catch_phrase", 
        "type": "string"
    },
    {
        "name": "ceo", 
        "type": "string"
    },
    {
        "name": "phone", 
        "type": "string"
    },
    {
        "name": "email", 
        "type": "string"
    },
    {
        "name": "exchange", 
        "type": "string"
    },
    {
        "name": "created_at",
        "type": "datetime",
        "notNull": True,
        "defaultValue": "now",
    }
]})
assert r.status_code == 200, "Unable to set table schema: %s" % r.json()

logging.info("setting table schema of table 'prices' ..")
r = xata.table().setTableSchema('prices', {"columns": [
    {
        "name": "symbol", 
        "type": "link",
        "link": {
            "table": "companies"
        }
    },
    {
        "name": "timestamp", 
        "type": "datetime"
    },
    {
        "name": "price", 
        "type": "float"
    },
    {
        "name": "delta", 
        "type": "float"
    },
    {
        "name": "percentage", 
        "type": "float"
    },
    {
        "name": "created_at",
        "type": "datetime",
        "notNull": True,
        "defaultValue": "now",
    }
]})
assert r.status_code == 200, "Unable to set table schema: %s" % r.json()

logging.info("done.")

