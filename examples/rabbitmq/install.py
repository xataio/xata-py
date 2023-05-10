#
# Licensed to Xatabase, Inc under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Xatabase, Inc licenses this file to you under the
# Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You
# may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import logging
import os

from dotenv import load_dotenv

from xata.client import XataClient

logging.basicConfig(format="%(asctime)s [%(process)d] %(levelname)s: %(message)s", level=logging.INFO)
load_dotenv()

XATA_DATABASE_NAME = os.environ.get("XATA_DB_NAME")
assert XATA_DATABASE_NAME, "env.XATA_DB_NAME not set."

xata = XataClient(db_name=XATA_DATABASE_NAME)

logging.info("checking credentials ..")
assert xata.users().getUser().status_code == 200, "Unable to connect to Xata. Please check credentials"

logging.info("checking if database exists ..")
r = xata.databases().getDatabaseMetadata(XATA_DATABASE_NAME)
if r.status_code == 404:
    logging.info("creating database '%s' .." % XATA_DATABASE_NAME)
    r = xata.databases().create(XATA_DATABASE_NAME, {"region": xata.get_config()["region"]})
    assert r.status_code == 201, "Unable to create database '%s': %s" % (XATA_DATABASE_NAME, r.json())
else:
    logging.info("database '%s' found, skipping creation step." % XATA_DATABASE_NAME)

logging.info("creating table 'companies' ..")
r = xata.table().create("companies")
assert r.status_code == 201, "Unable to create table 'companies'"

logging.info("creating table 'prices' ..")
r = xata.table().create("prices")
assert r.status_code == 201, "Unable to create table 'prices'"

logging.info("setting table schema of table 'companies' ..")
r = xata.table().setSchema(
    "companies",
    {
        "columns": [
            {"name": "name", "type": "string"},
            {"name": "address", "type": "string"},
            {"name": "catch_phrase", "type": "string"},
            {"name": "ceo", "type": "string"},
            {"name": "phone", "type": "string"},
            {"name": "email", "type": "string"},
            {"name": "exchange", "type": "string"},
            {
                "name": "created_at",
                "type": "datetime",
                "notNull": True,
                "defaultValue": "now",
            },
        ]
    },
)
assert r.status_code == 200, "Unable to set table schema: %s" % r.json()

logging.info("setting table schema of table 'prices' ..")
r = xata.table().setSchema(
    "prices",
    {
        "columns": [
            {"name": "symbol", "type": "link", "link": {"table": "companies"}},
            {"name": "timestamp", "type": "datetime"},
            {"name": "price", "type": "float"},
            {"name": "delta", "type": "float"},
            {"name": "percentage", "type": "float"},
            {
                "name": "created_at",
                "type": "datetime",
                "notNull": True,
                "defaultValue": "now",
            },
        ]
    },
)
assert r.status_code == 200, "Unable to set table schema: %s" % r.json()

logging.info("setup done.")
cfg = xata.get_config()
url = "https://app.xata.io/workspaces/%s/dbs/%s:%s" % (cfg["workspaceId"], cfg["dbName"], cfg["region"])
logging.info(f"UI: {url}")
logging.info("`make run` to start generating events")
