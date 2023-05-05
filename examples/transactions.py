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

from xata.client import XataClient
from xata.helpers import Transaction

# We can now add insert, update, delete or get operations to
# the transaction helper. Max is 1000, if you exceed this treshold
# an exception is thrown.
# Please ensure the SDK version is > 0.10.0

xata = XataClient()
trx = Transaction(xata)

# We want to get records by their id from the table "Avengers"
trx.get("Marvel", "hulk")
trx.get("Marvel", "spiderman", ["name"]) # return the name column
trx.get("Marvel", "dr_who") # this record does not exist

# Update the record of batman in table DC
trx.update("DC", "batman", {"name": "Bruce Wayne"})

# Delete records from different tables
trx.delete("nba_teams", "seattle_super_sonics")
trx.delete("nba_teams", "fc_barcelona", ["name", "city"]) # return two fields before deleting
trx.delete("nba_teams", "dallas_cowboys") # does not exist

# Insert records
trx.insert("Marvel", {"id": "dr_strange", "name": "Dr Stephen Strange"})

# How many transaction operations are scheduled?
print(trx.size())
# 8

# Run the Transactions
result = trx.run()
print(result)

# The order of the results is the same as the transaction operations
# Meaning, you can always reference back.
# {
# 'status_code': 200, 
# 'results': [
#   {'columns': {'id': 'hulk', 'xata': {'version': 0}}, 'operation': 'get'}, 
#   {'columns': {'name': 'Peter Parker'}, 'operation': 'get'}, 
#   {'columns': {}, 'operation': 'get'}, 
#   {'columns': {}, 'id': 'batman', 'operation': 'update', 'rows': 1}, 
#   {'operation': 'delete', 'rows': 1}, 
#   {'columns': {'city': 'Barcelona', 'name': 'FC Barcelona'}, 'operation': 'delete', 'rows': 1}, 
#   {'operation': 'delete', 'rows': 0}, 
#   {'columns': {}, 'id': 'dr_strange', 'operation': 'insert', 'rows': 1}
# ], 
# 'has_errors': False, 
# 'errors': []
# }

# Transactions are flushed after a `run`
print(trx.size())
# 0

# If the transaction has error it's aborted and no operation is executed
trx.insert("Marvel", {"field_that_does_not_exist": "superpower"})
trx.get("Marvel", "hulk")
results = trx.run()

print(results)
# {
# 'status_code': 400, 
# 'results': [], 
# 'has_errors': True, 
# 'errors': [
#   {'index': 0, 'message': 'table [Marvel]: invalid record: column [field_that_does_not_exist]: column not found'}
# ]
# }
