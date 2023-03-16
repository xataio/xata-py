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

import random

from xata import XataClient

client = XataClient(db_url="https://xata-uq2d57.eu-west-1.xata.sh/db/nextjsconf22", branch_name="main")


more = True
cursor = None
emails = []
while more:
    resp = client.query("emails", page=dict(size=200, after=cursor))
    more = resp["meta"]["page"].get("more", False)
    cursor = resp["meta"]["page"]["cursor"]
    emails.extend(rec["email"] for rec in resp["records"])

winners = random.sample(emails, 50)

for winner in winners:
    print(winner)
