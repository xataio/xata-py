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
from datetime import datetime, timezone
from threading import Lock, Thread

from .client import XataClient

BP_DEFAULT_THREAD_POOL_SIZE = 4
BP_DEFAULT_BATCH_SIZE = 25
BP_DEFAULT_FLUSH_INTERVAL = 5
BP_DEFAULT_PROCESSING_TIMEOUT = 0.025
BP_DEFAULT_THROW_EXCEPTION = False
TRX_MAX_OPERATIONS = 1000


class BulkProcessor(object):
    """
    Additional abstraction for bulk requests that process'
    requests in parallel
    :stability beta
    """

    def __init__(
        self,
        client: XataClient,
        thread_pool_size: int = BP_DEFAULT_THREAD_POOL_SIZE,
        batch_size: int = BP_DEFAULT_BATCH_SIZE,
        flush_interval: int = BP_DEFAULT_FLUSH_INTERVAL,
        processing_timeout: float = BP_DEFAULT_PROCESSING_TIMEOUT,
        throw_exception: bool = BP_DEFAULT_THROW_EXCEPTION,
    ):
        """
        BulkProcessor: Abstraction for bulk ingestion of records.

        :stability beta

        :param client: XataClient
        :param thread_pool_size: int How many data queue workers should be deployed (default: 4)
        :param batch_size: int How many records per table should be pushed as batch (default: 25)
        :param flush_interval: int After how many seconds should the per table queue be flushed (default: 5 seconds)
        :processing_timeout: float Cooldown period between batches (default: 0.025 seconds)
        :throw_exception: bool Throw exception ingestion, could kill all workers (default: False)

        :raises Exception if throw exception is enabled
        """
        if thread_pool_size < 1:
            raise Exception("thread pool size must be greater than 0, default: %d" % BP_DEFAULT_THREAD_POOL_SIZE)
        if processing_timeout < 0:
            raise Exception("processing timeout can not be negative, default: %f" % BP_DEFAULT_PROCESSING_TIMEOUT)
        if flush_interval < 0:
            raise Exception("flush interval can not be negative, default: %f" % BP_DEFAULT_FLUSH_INTERVAL)
        if batch_size < 1:
            raise Exception("batch size can not be less than one, default: %d" % BP_DEFAULT_BATCH_SIZE)

        self.client = client
        self.processing_timeout = processing_timeout
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.failed_batches_queue = []
        self.throw_exception = throw_exception
        self.stats = {"total": 0, "queue": 0, "failed_batches": 0, "tables": {}}
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        self.thread_workers = []
        self.records = self.Records(self.batch_size, self.flush_interval, self.logger)

        for i in range(thread_pool_size):
            worker = Thread(target=self.process, daemon=True, args=(i,), name="worker-%d" % i)
            worker.start()
            self.thread_workers.append(worker)

    def process(self, id: int):
        """
        Process the records
        """
        self.logger.debug(
            "thread #%d: starting bulk processor [thread_pool=%d, batch=%d, flush=%d, processing_timeout=%s"
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
                try:
                    r = self.client.records().bulkInsertTableRecords(batch["table"], {"records": batch["records"]})
                    if r.status_code != 200:
                        self.logger.error(
                            "thread #%d: unable to process batch for table '%s', with error: %d - %s"
                            % (id, batch["table"], r.status_code, r.json())
                        )
                        # Add to failed queue
                        self.failed_batches_queue.append(
                            {
                                "timestamp": datetime.utcnow(),
                                "records": batch["records"],
                                "table": batch["table"],
                                "response": r,
                            }
                        )
                        self.stats["failed_batches"] += 1
                        if self.throw_exception:
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
                except Exception as exc:
                    logging.error("thread #%d: %s" % (id, exc))
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

    def get_failed_batches(self) -> list[dict]:
        """
        Get the batched records that could not be processed with the error
        :return list[dict]
        """
        return self.failed_batches_queue

    def get_stats(self):
        """
        Get processing statistics

        :return dict
        """
        return self.stats

    def flush_queue(self):
        """
        Flush all records from the queue.
        """
        self.logger.debug("flushing queue with %d records .." % (self.records.size()))
        self.records.set_flush_interval(0)
        self.processing_timeout = 0

        # If the queue is not empty wait for one flush interval.
        # Purpose is a race condition with self.stats["queue"]
        if self.records.size() > 0:
            time.sleep(self.flush_interval)

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
                if len(self.store[table_name]["records"]) >= self.batch_size or flush_needed:
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


def to_rfc339(dt: datetime, tz=timezone.utc) -> str:
    """
    Format a datetime object to an RFC3339 compliant string
    :link https://xata.io/docs/concepts/data-model#datetime
    :link https://datatracker.ietf.org/doc/html/rfc3339

    :param dt: datetime instance to convert
    :param tz: timezone to convert in, default: UTC
    :return str
    """
    return dt.replace(tzinfo=tz).isoformat()


class Transaction(object):
    """
    Additional abstraction for bulk requests that process'
    requests in parallel
    :stability beta
    """

    def __init__(
        self,
        client: XataClient,
    ) -> None:
        """
        Transaction Helper
        Wrapper to simplify running transactions

        :stability beta

        :param client: XataClient
        """
        self.client = client

        self.has_run = False
        self.operations = {"operations": []}

    def _add_operation(self, operation: dict) -> None:
        if len(self.operations["operations"]) >= TRX_MAX_OPERATIONS:
            raise Exception(f"Maximum amount of {TRX_MAX_OPERATIONS} transaction operations exceeded.")
        self.operations["operations"].append(operation)

    def insert(self, table: str, record: dict, createOnly: bool = False) -> None:
        """
        Inserts can be used to insert records across any number of tables in your database. As with the
        insert endpoints, you can explicitly set an ID, or omit it and have Xata auto-generate one for you.
        Either way, on a successful transaction, Xata will return the ID to you.

        :param table: str
        :param record: dict
        :param createOnly: bool By default, if a record exists with the same explicit ID, Xata will overwrite
            the record. You can adjust this behavior by setting `createOnly` to `true` for the operation. Default: False

        :raises Exception if limit of 1000 operations is exceeded
        """
        self._add_operation({"insert": {"table": table, "record": record, "createOnly": createOnly}})

    def update(self, table: str, recordId: str, fields: dict, upsert: bool = False) -> None:
        """
        Updates can be used to update records in any number of tables in your database. The update operation
        requires an ID parameter explicitly defined. The operation will only replace the fields explicitly
        specified in your operation. The update operation also supports the upsert flag. Off by default, but
        if set to `true`, the update operation will insert the record if no record is found with the provided ID.

        :param table: str
        :param recordId: str
        :param fields: dict
        :param upsert: bool Default: False

        :raises Exception if limit of 1000 operations is exceeded
        """
        self._add_operation({"update": {"table": table, "id": recordId, "fields": fields, "upsert": upsert}})

    def delete(self, table: str, recordId: str, columns: list[str] = []) -> None:
        """
        A delete is used to remove records. Delete can operate on records from the same transaction, and will
        not cancel a transaction if no record is found.

        :param table: str
        :param recordId: str
        :param columns: list of columns to retrieve

        :raises Exception if limit of 1000 operations is exceeded
        """
        self._add_operation({"delete": {"table": table, "id": recordId, "columns": columns}})

    def get(self, table: str, recordId: str, columns: list[str] = []) -> None:
        """
        A get is used to retrieve a record by id. A get operation can retrieve records created in the same transaction
        but will not cancel a transaction if no record is found.

        :param table: str
        :param recordId: str
        :param columns: list of columns to retrieve

        :raises Exception if limit of 1000 operations is exceeded
        """
        self._add_operation({"get": {"table": table, "id": recordId, "columns": columns}})

    def run(self) -> dict:
        """
        Commit the transactions. Flushes the operations queue

        :return dict
        """
        r = self.client.records().branchTransaction(self.operations)
        result = {
            "status_code": r.status_code,
            "results": r.json()["results"] if "results" in r.json() else [],
            "has_errors": True if "errors" in r.json() else False,
            "errors": r.json()["errors"] if "errors" in r.json() else [],
        }
        self.operations["operations"] = [] # free memory
        return result

    def size(self) -> int:
        """
        Get amount of operations in queue
        :return int
        """
        return len(self.operations["operations"])
