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

from faker import Faker

from xata import XataClient

fake = Faker()


# client = XataClient(base_url_domain="staging.xatabase.co")
client = XataClient()


def load_data():
    for i in range(100):
        records = [
            {
                "name": fake.name(),
                "email": fake.unique.email(),
                "description": fake.text(),
                "age": fake.random_int(min=0, max=100),
                "is_active": fake.boolean(),
                "created_at": fake.date_time(),
                "icon": fake.image_url(),
            }
            for i in range(1000)
        ]

        res = client.post(
            "/db/heavyTable:main/tables/users/bulk", json={"records": records}
        )
        if res.status_code != 200:
            print(res.text)
            raise Exception("Failed to load data")
        print(".")


# run load_data in 10 parallel threads

threads = []
for i in range(10):
    t = threading.Thread(target=load_data)
    t.start()
    threads.append(t)
for x in threads:
    x.join()
