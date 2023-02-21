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
import time
from threading import Lock, Thread

from .client import XataClient

DEFAULT_THREAD_POOL_SIZE = 4
DEFAULT_BATCH_SIZE = 25
DEFAULT_FLUSH_INTERVAL = 5
DEFAULT_PROCESSING_TIMEOUT = 0.025


class BulkProcessor(object):
    """
    !!! This helper is still work in progress !!!

    Additional abstraction for bulk requests that process'
    requests in parallel
    :stability alpha
    """

    def __init__(
        self,
        client: XataClient,
        thread_pool_size: int = DEFAULT_THREAD_POOL_SIZE,
        batch_size: int = DEFAULT_BATCH_SIZE,
        flush_interval: int = DEFAULT_FLUSH_INTERVAL,
        processing_timeout: float = DEFAULT_PROCESSING_TIMEOUT,
    ):
        """
        BulkProcessor: Abstraction for bulk ingestion of records.

        :stability alpha


        :param client: XataClient
        :param thread_pool_size: int
        :param batch_size: int
        :param flush_interval: int
        :processing_timeout: float
        """
        if thread_pool_size < 1:
            raise Exception(
                "thread pool size must be greater than 0, default: %d"
                % DEFAULT_THREAD_POOL_SIZE
            )
        if processing_timeout < 0:
            raise Exception(
                "processing timeout can not be negative, default: %f"
                % DEFAULT_PROCESSING_TIMEOUT
            )

        self.client = client
        self.processing_timeout = processing_timeout
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.stats = {"total": 0, "queue": 0, "tables": {}}
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        self.thread_workers = []
        self.records = self.Records(self.batch_size, self.flush_interval, self.logger)

        for i in range(thread_pool_size):
            worker = Thread(
                target=self.process, daemon=True, args=(i,), name="worker-%d" % i
            )
            worker.start()
            self.thread_workers.append(worker)

    def process(self, id: int):
        """
        Process the records
        """
        self.logger.debug(
            "thread #%d: starting bulk processor [thread_pool_size=%d, batch_size=%d, flush_interval=%d, processing_timeout=%s"
            % (
                id,
                len(self.thread_workers),
                self.batch_size,
                self.flush_interval,
                self.processing_timeout,
            )
        )
        while True:
            batch = self.records.next_batch()
            if "table" in batch and len(batch["records"]) > 0:
                r = self.client.records().bulkInsertTableRecords(
                    batch["table"], {"records": batch["records"]}
                )
                if r.status_code != 200:
                    self.logger.error(
                        "thread #%d: unable to process batch for table '%s', with error: %d - %s"
                        % (id, batch["table"], r.status_code, r.json())
                    )
                    # TODO add records to batch again or callback
                    raise Exception(r.json())

                self.logger.debug(
                    "thread #%d: pushed a batch of %d records to table %s"
                    % (id, len(batch["records"]), batch["table"])
                )
                self.stats["total"] += len(batch["records"])
                self.stats["queue"] = self.records.size()
                if batch["table"] not in self.stats["tables"]:
                    self.stats["tables"][batch["table"]] = 0
                self.stats["tables"][batch["table"]] += len(batch["records"])
            time.sleep(self.processing_timeout)

    def put_record(self, table_name: str, record: dict):
        """
        Put a record to the processing queue

        :param table_name: str
        :param record: dict
        """
        self.records.put(table_name, [record])

    def put_records(self, table_name: str, records: list[dict]):
        """
        Put multtiple records to the processing queue

        :param table_name: str
        :param records: list[dict]
        """
        self.records.put(table_name, records)

    def dlq(self):
        """
        TODO
        """
        pass

    def get_stats(self):
        """
        Get processing statistics

        :return dict
        """
        return self.stats

    def flush_queue(self):
        """
        Flush all records from the queue.
        TODO: this method does not work correctly yet. The queue is fully flushed
        """
        self.logger.debug("flushing queue with %d records .." % (self.records.size()))
        self.records.set_flush_interval(0)
        self.processing_timeout = 0

        while self.stats["queue"] > 0:
            self.logger.debug("flushing queue with %d records." % self.stats["queue"])
            time.sleep(self.processing_timeout / len(self.thread_workers) + 0.01)

    class Records(object):
        """
        Thread safe storage for records to persist by the bulk processor
        """

        def __init__(self, batch_size: int, flush_interval: int, logger):
            """
            :param batch_size: int
            :param flush_interval: int
            """
            self.batch_size = batch_size
            self.flush_interval = flush_interval
            self.logger = logger

            self.store = dict()
            self.store_ptr = 0
            self.lock = Lock()

        def set_flush_interval(self, interval: int):
            self.flush_interval = interval

        def put(self, table_name: str, records: list[dict]):
            """
            :param table_name: str
            :param records: list[dict]
            """
            with self.lock:
                if table_name not in self.store.keys():
                    self.store[table_name] = {
                        "lock": Lock(),
                        "flushed": time.time(),
                        "records": list(),
                    }
            with self.store[table_name]["lock"]:
                self.store[table_name]["records"] += records

        def next_batch(self) -> dict:
            """
            Get the next batch of records to persist

            :return dict
            """
            table_name = ""
            with self.lock:
                names = list(self.store.keys())
                if len(names) == 0:
                    return {}

                self.store_ptr += 1
                if len(names) <= self.store_ptr:
                    self.store_ptr = 0
                table_name = names[self.store_ptr]

            rs = []
            with self.store[table_name]["lock"]:
                # flush interval exceeded
                time_elapsed = time.time() - self.store[table_name]["flushed"]
                flush_needed = time_elapsed > self.flush_interval
                if flush_needed and len(self.store[table_name]["records"]) > 0:
                    self.logger.debug(
                        "flushing table '%s' with %d records after interval %s > %d"
                        % (
                            table_name,
                            len(self.store[table_name]["records"]),
                            time_elapsed,
                            self.flush_interval,
                        )
                    )
                # pop records ?
                if (
                    len(self.store[table_name]["records"]) >= self.batch_size
                    or flush_needed
                ):
                    self.store[table_name]["flushed"] = time.time()
                    rs = self.store[table_name]["records"][0 : self.batch_size]
                    del self.store[table_name]["records"][0 : self.batch_size]
            return {"table": table_name, "records": rs}

        def length(self, table_name: str) -> int:
            """
            Get record count of a table

            :param table_name: str
            """
            with self.store[table_name]["lock"]:
                return len(self.store[table_name]["records"])

        def size(self) -> int:
            """
            Get total size of stored records
            """
            with self.lock:
                return sum([len(self.store[n]["records"]) for n in self.store.keys()])
