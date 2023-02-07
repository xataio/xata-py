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

import threading
import time

from xata import XataClient
from xata.client import RateLimitException

client = XataClient()
# client = XataClient(base_url_domain="staging.xatabase.co")


def run_query_loop(threadId):
    for i in range(100):
        print(f"Thread {threadId} query {i}")
        try:
            res = client.query(
                "titles",
                db_name="imdb",
                branch_name="main",
                filter={"primaryTitle": {"$contains": "testingtest"}},
            )

            if res.status_code != 200:
                raise Exception("Failed to query")
            print(res.json())
        except RateLimitException as e:
            print(f"Rate limited {threadId}:", e)
            time.sleep(1)


threads = []
for i in range(50):
    t = threading.Thread(target=run_query_loop, args=(i,))
    t.start()
    threads.append(t)
for x in threads:
    x.join()
