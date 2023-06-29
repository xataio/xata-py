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

from faker import Faker

from xata.client import XataClient
from xata.helpers import BulkProcessor

# This example demonstrates how to use the BulkProcessor.
# We are going to generate 500 records whith random data and ingest
# them to Xata with the BulkProcessor
#
# @link https://xata.io/docs/python-sdk/bulk-processor

fake = Faker()
xata = XataClient()
bp = BulkProcessor(xata)


def generate_records(n: int):
    return [
        {
            "name": fake.name(),
            "email": fake.unique.email(),
            "age": fake.random_int(min=0, max=100),
            "is_active": fake.boolean(),
            "icon": fake.image_url(),
        }
        for i in range(n)
    ]


# Generate records
records = generate_records(500)

# Put multiple records to the ingestion queue
bp.put_records("people", records)

# Put a single records
records = generate_records(2)
bp.put_record("people", records[0])
bp.put_record("people", records[1])

# Ensure the queue is flushed before exiting the process
bp.flush_queue()

# Execution stats
print(bp.get_stats())
# {
#   'total': 502,
#   'queue': 0,
#   'failed_batches': 0,
#   'tables': {
#       'people': 502
#   }
# }
