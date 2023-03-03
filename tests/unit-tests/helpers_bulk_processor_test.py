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

import unittest

import pytest

from xata.client import XataClient
from xata.helpers import BulkProcessor


class TestHelpersBulkProcessor(unittest.TestCase):
    def test_bulk_processor_init(self):
        client = XataClient(api_key="api_key", workspace_id="ws_id")

        with pytest.raises(Exception) as e:
            bp = BulkProcessor(client, thread_pool_size=-1)
        assert str(e.value) == "thread pool size must be greater than 0, default: 4"

        with pytest.raises(Exception) as e:
            bp = BulkProcessor(client, batch_size=-1)
        assert str(e.value) == "batch size can not be less than one, default: 25"

        with pytest.raises(Exception) as e:
            bp = BulkProcessor(client, flush_interval=-1)
        assert str(e.value) == "flush interval can not be negative, default: 5.000000"

        with pytest.raises(Exception) as e:
            bp = BulkProcessor(client, processing_timeout=-1)
        assert (
            str(e.value) == "processing timeout can not be negative, default: 0.025000"
        )

    def test_bulk_processor_stats(self):
        client = XataClient(api_key="api_key", workspace_id="ws_id")
        bp = BulkProcessor(client)
        sts = bp.get_stats()

        assert "total" in sts
        assert "queue" in sts
        assert "failed_batches" in sts
        assert "tables" in sts
        assert sts["total"] == 0
        assert sts["queue"] == 0
        assert sts["failed_batches"] == 0
        assert sts["tables"] == {}
