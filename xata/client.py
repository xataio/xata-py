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

import json
import os
import uuid
from typing import Literal, Optional
from urllib.parse import urljoin

import deprecation
import requests
from dotenv import dotenv_values

from .errors import (
    BadRequestException,
    RateLimitException,
    RecordNotFoundException,
    ServerErrorException,
    UnauthorizedException,
)
from .namespaces.core.authentication import Authentication
from .namespaces.core.databases import Databases
from .namespaces.core.invites import Invites
from .namespaces.core.users import Users
from .namespaces.core.workspaces import Workspaces
from .namespaces.workspace.branch import Branch
from .namespaces.workspace.migrations import Migrations
from .namespaces.workspace.records import Records
from .namespaces.workspace.search_and_filter import Search_and_filter
from .namespaces.workspace.table import Table

# TODO this is a manual task, to keep in sync with pyproject.toml
# could/should be automated to keep in sync
__version__ = "0.10.1"

PERSONAL_API_KEY_LOCATION = "~/.config/xata/key"
DEFAULT_DATA_PLANE_DOMAIN = "xata.sh"
DEFAULT_CONTROL_PLANE_DOMAIN = "api.xata.io"
DEFAULT_REGION = "us-east-1"
DEFAULT_BRANCH_NAME = "main"
CONFIG_LOCATION = ".xatarc"

ApiKeyLocation = Literal["env", "dotenv", "profile", "parameter"]
WorkspaceIdLocation = Literal["parameter", "env", "config"]


class XataClient:
    """This is the Xata Client. When initialized, it will attempt to read the relevant
    configuration (API key, workspace ID, database name, branch name) from the following
    sources in order:

    * parameters passed to the constructor
    * environment variables
    * .env file
    * .xatarc configuration file

    :meta public:
    :param api_key: API key to use for authentication.
    :param db_url: The database URL to use. If this is specified,
                   then workspace_id, region and db_name must not be specified.
    :param workspace_id: The workspace ID to use.
    :param region: The region to use.
    :param db_name: The database name to use.
    :param branch_name: The branch name to use. Defaults to `main`
    :param domain_core: The domain to use for "core", the control plane. Defaults to api.xata.io.
    :param domain_workspace: The domain to use for "workspace", data plane. Defaults to xata.sh.
    """

    configRead: bool = False
    config = None
    namespaces = {}  # lazy loading container for the namespaces

    def __init__(
        self,
        api_key: str = None,
        region: str = DEFAULT_REGION,
        workspace_id: str = None,
        db_name: str = None,
        db_url: str = None,
        branch_name: str = DEFAULT_BRANCH_NAME,
        domain_core: str = DEFAULT_CONTROL_PLANE_DOMAIN,
        domain_workspace: str = DEFAULT_DATA_PLANE_DOMAIN,
    ):
        """
        Constructor for the XataClient.
        """
        if db_url is not None or os.environ.get("XATA_DATABASE_URL", None) is not None:
            if workspace_id is not None or db_name is not None:
                raise Exception("Cannot specify both db_url and workspace_id/region/db_name")
            if db_url is None:
                db_url = os.environ.get("XATA_DATABASE_URL")
            workspace_id, region, db_name, branch_name, domain_workspace = self._parse_database_url(db_url)

        if api_key is None:
            self.api_key, self.api_key_location = self._get_api_key()
        else:
            self.api_key, self.api_key_location = api_key, "parameter"

        if workspace_id is None:
            (
                self.workspace_id,
                self.region,
                self.workspace_id_location,  # TODO not sure if we need the location at all
            ) = self._get_workspace_id()
        else:
            self.workspace_id = workspace_id
            self.workspace_id_location = "parameter"
            self.region = region

        # TODO remove these assignment once client.request is removed from the codebase
        self.base_url = f"https://{self.workspace_id}.{self.region}.{domain_workspace}"
        self.control_plane_url = f"https://{domain_core}/workspaces/{self.workspace_id}/"

        self.db_name = self.get_database_name_if_configured() if db_name is None else db_name
        self.branch_name = self.get_branch_name_if_configured() if branch_name is None else branch_name

        self.domain_core = domain_core
        self.domain_workspace = domain_workspace

        # init default headers
        self.headers = {
            "authorization": f"Bearer {self.api_key}",
            "user-agent": f"xataio/xata-py:{__version__}",
            "x-xata-client-id": str(uuid.uuid4()),
            "x-xata-session-id": str(uuid.uuid4()),
            "x-xata-agent": f"client=PY_SDK;version={__version__};",
        }

        # init namespaces
        self._authentication = Authentication(self)
        self._branch = Branch(self)
        self._search_and_filter = Search_and_filter(self)
        self._databases = Databases(self)
        self._invites = Invites(self)
        self._migrations = Migrations(self)
        self._records = Records(self)
        self._table = Table(self)
        self._users = Users(self)
        self._workspaces = Workspaces(self)

    def get_config(self) -> dict:
        """
        Get the configuration
        """
        return {
            "apiKey": self.api_key,
            "apiKeyLocation": self.api_key_location,
            "workspaceId": self.workspace_id,
            "region": self.region,
            "dbName": self.db_name,
            "branchName": self.branch_name,
            "version": __version__,
            "domain_core": self.domain_core,
            "domain_workspace": self.domain_workspace,
        }

    def get_headers(self) -> dict:
        """
        Get the static headers that are iniatilized on client init.
        """
        return self.headers

    def set_header(self, name: str, value: str):
        """
        Set a header value. Every name is lower case and overwrites existing values.
        This can be useful for some proxies that require certain headers.

        :param name: str
        :param value: str
        """
        self.headers[name.lower().strip()] = value

    def delete_header(self, name: str) -> bool:
        """
        Delete a header from the scope. A header name will be lowercased.
        :param name: str
        :return bool
        """
        name = name.lower().strip()
        if name not in self.headers:
            return False
        del self.headers[name]
        return True

    def _get_api_key(self) -> tuple[str, ApiKeyLocation]:
        if os.environ.get("XATA_API_KEY") is not None:
            return os.environ.get("XATA_API_KEY"), "env"

        envVals = dotenv_values(".env")
        if envVals.get("XATA_API_KEY") is not None:
            return envVals.get("XATA_API_KEY"), "dotenv"

        if os.path.isfile(os.path.expanduser(PERSONAL_API_KEY_LOCATION)):
            with open(os.path.expanduser(PERSONAL_API_KEY_LOCATION), "r") as f:
                return f.read().strip(), "profile"

        raise Exception(
            f"No API key found. Searched in `XATA_API_KEY` env, "
            f"`{PERSONAL_API_KEY_LOCATION}`, and `{os.path.abspath('.env')}`"
        )

    def _get_workspace_id(self) -> tuple[str, str, WorkspaceIdLocation]:
        if os.environ.get("XATA_WORKSPACE_ID") is not None:
            return (
                os.environ.get("XATA_WORKSPACE_ID"),
                os.environ.get("XATA_REGION", DEFAULT_REGION),
                "env",
            )

        envVals = dotenv_values(".env")
        if envVals.get("XATA_WORKSPACE_ID") is not None:
            return (
                envVals.get("XATA_WORKSPACE_ID"),
                envVals.get("XATA_REGION", DEFAULT_REGION),
                "dotenv",
            )

        self.ensure_config_read()
        if self.config is not None and self.config.get("databaseURL"):
            workspaceID, region, _, _, _ = self._parse_database_url(self.config.get("databaseURL"))
            return workspaceID, region, "config"
        raise Exception(
            f"No workspace ID found. Searched in `XATA_WORKSPACE_ID` env, "
            f"`{PERSONAL_API_KEY_LOCATION}`, and `{os.path.abspath('.env')}`"
        )

    def get_database_name_if_configured(self) -> str:
        self.ensure_config_read()
        if self.config is not None and self.config.get("databaseURL"):
            _, _, db_name, _, _ = self._parse_database_url(self.config.get("databaseURL"))
            return db_name
        return None

    def get_branch_name_if_configured(self) -> str:
        # TODO: resolve branch name by the current git branch
        return os.environ.get("XATA_BRANCH")

    def get_db_branch_name(self, db_name: str = None, branch_name: str = None) -> str:
        """
        Get Database with branch name, format: {db_name}:{branch_name}
        The name can be build with passed params or from config or a combination of both

        :param db_name: str
        :branch_name: str

        :return str
        """
        if db_name is None:
            db_name = self.db_name
        if branch_name is None:
            branch_name = self.branch_name
        return f"{db_name}:{branch_name}"

    def set_db_and_branch_names(self, db_name: str = None, branch_name: str = None):
        """
        Set either or both the database - or the branch name

        :param db_name: str
        :param branch_name: str
        """
        if db_name is not None:
            self.db_name = db_name
        if branch_name is not None:
            self.branch_name = branch_name

    def request(self, method, urlPath, cp=False, headers={}, expect_codes=[], **kwargs):
        headers = {
            **headers,
            **self.headers,
        }  # TODO use "|" when client py min version >= 3.9

        base_url = self.base_url if not cp else self.control_plane_url
        url = urljoin(base_url, urlPath.lstrip("/"))

        resp = requests.request(method, url, headers=headers, **kwargs)
        if resp.status_code > 299:
            if resp.status_code in expect_codes:
                return resp
            if resp.status_code == 401:
                raise UnauthorizedException(f"Unauthorized: {resp.json()} API key location: {self.api_key_location}")
            elif resp.status_code == 429:
                raise RateLimitException(f"Rate limited: {resp.json()}")
            elif resp.status_code >= 399 and resp.status_code < 500:
                raise BadRequestException(resp.status_code, resp.json().get("message"))
            elif resp.status_code >= 500:
                raise ServerErrorException(f"Server error: {resp.text}")
            raise Exception(f"{resp.status_code} {resp.text}")
        return resp

    def ensure_config_read(self) -> bool:
        if self.configRead:
            return False
        if os.path.isfile(CONFIG_LOCATION):
            with open(CONFIG_LOCATION, "r") as f:
                self.config = json.load(f)
        self.configRead = True
        return True

    def _parse_database_url(self, databaseURL: str) -> tuple[str, str, str, str, str]:
        """
        Parse Database URL
        Branch name is optional.
        Format: https://{workspace_id}.{region}.xata.sh/db/{db_name}:{branch_name}
        :return workspace_id, region, db_name, branch_name, domain
        """
        (_, _, host, _, db_branch_name) = databaseURL.split("/")
        if host == "" or db_branch_name == "":
            raise Exception(
                "Invalid database URL: '%s', format: 'https://{workspace_id}.{region}.xata.sh/db/{db_name}:{branch_name}' expected."
                % databaseURL
            )
        # split host {workspace_id}.{region}
        host_parts = host.split(".")
        if len(host_parts) < 4:
            raise Exception(
                "Invalid format for workspaceId and region in the URL: '%s', expected: 'https://{workspace_id}.{region}.xata.sh/db/{db_name}:{branch_name}'"
                % databaseURL
            )
        # build domain name
        domain = ".".join(host_parts[2:])
        # split {db_name}:{branch_name}
        db_branch_parts = db_branch_name.split(":")
        if len(db_branch_parts) == 2 and db_branch_parts[1] != "":
            # branch defined and not empty, return it!
            return host_parts[0], host_parts[1], db_branch_parts[0], db_branch_parts[1], domain
        # does not have a branch defined
        return host_parts[0], host_parts[1], db_branch_parts[0], DEFAULT_BRANCH_NAME, domain

    @deprecation.deprecated(
        deprecated_in="0.7.0",
        removed_in="1.0",
        current_version=__version__,
        details="No direct replacement. Method is obsolete",
    )
    def request_body_from_params(
        self,
        columns: list[str] = None,
        filter: dict = None,
        sort: dict = None,
        page: dict = None,
    ) -> dict:
        """
        DEPRECATED, this method will be removed with the 1.0.0 release
        """
        body = {}
        if columns is not None:
            body["columns"] = columns
        if filter is not None:
            body["filter"] = filter
        if sort is not None:
            body["sort"] = sort
        if page is not None:
            body["page"] = page
        return body

    @deprecation.deprecated(
        deprecated_in="0.7.0",
        removed_in="1.0",
        current_version=__version__,
        details="No direct replacement. Method is obsolete",
    )
    def db_and_branch_names_from_params(self, db_name, branch_name) -> tuple[str, str]:
        """
        DEPRECATED, this method will be removed with the 1.0.0 release
        """
        db_name = db_name or self.db_name
        branch_name = branch_name or self.branch_name
        if db_name is None:
            raise Exception("Database name is not configured. Please set it via `xata init` or pass it as a parameter.")
        if branch_name is None:
            raise Exception(
                "Branch name is not configured. Please set it in the `XATA_BRANCH` env var or pass it as a parameter."
            )
        return db_name, branch_name

    @deprecation.deprecated(
        deprecated_in="0.7.0",
        removed_in="1.0",
        current_version=__version__,
        details="client.search_and_filter().queryTable(:table, :body)",
    )
    def query(
        self,
        table: str,
        db_name: str = None,
        branch_name: str = None,
        columns: list[str] = None,
        filter: dict = None,
        sort: dict = None,
        page: dict = None,
    ) -> dict:
        """
        Please use: client.search_and_filter().queryTable()

        :meta public:
        :param table: The name of the table to query.
        :param db_name: The name of the database to query. If not provided, the database name
                    from the client obejct is used.
        :param branch_name: The name of the branch to query. If not provided, the branch name
                        from the client obejct is used.
        :param columns: A list of column names to return. If not provided, all columns are returned.
        :param filter: A filter expression to apply to the query.
        :param sort: A sort expression to apply to the query.
        :param page: A page expression to apply to the query.
        :return: A page of results.
        """
        db_name, branch_name = self.db_and_branch_names_from_params(db_name, branch_name)
        body = self.request_body_from_params(columns, filter, sort, page)
        result = self.request("POST", f"/db/{db_name}:{branch_name}/tables/{table}/query", json=body)
        return result.json()

    @deprecation.deprecated(
        deprecated_in="0.7.0",
        removed_in="1.0",
        current_version=__version__,
        details='No direct replacement. Use `{"size": 1}` with client.search_and_filter().queryTable()',
    )
    def get_first(
        self,
        table: str,
        db_name: str = None,
        branch_name: str = None,
        columns: list[str] = None,
        filter: dict = None,
        sort: dict = None,
    ) -> dict:
        """Get the first record from a table respecting the provided filters and sort order.

        :meta public:
        :param table: The name of the table to query.
        :param db_name: The name of the database to query. If not provided, the database name
                        from the client obejct is used.
        :param branch_name: The name of the branch to query. If not provided, the branch name
                        from the client obejct is used.
        :param columns: A list of column names to return. If not provided, all columns are returned.
        :param filter: A filter expression to apply to the query.
        :param sort: A sort expression to apply to the query.
        :return: A record as a dictionary.
        """
        page = {"size": 1}
        db_name, branch_name = self.db_and_branch_names_from_params(db_name, branch_name)
        body = self.request_body_from_params(columns, filter, sort, page)
        result = self.request("POST", f"/db/{db_name}:{branch_name}/tables/{table}/query", json=body)
        data = result.json()
        if len(data.get("records", [])) == 0:
            return None
        return data.get("records")[0]

    @deprecation.deprecated(
        deprecated_in="0.7.0",
        removed_in="1.0",
        current_version=__version__,
        details="client.records().getRecord(:table, :id)",
    )
    def get_by_id(
        self,
        table: str,
        id: str,
        db_name: str = None,
        branch_name: str = None,
    ) -> Optional[dict]:
        """
        Please use: client.records().getRecord(<id>)

        :param table: The name of the table to query.
        :param id: The ID of the record to get.
        :param db_name: The name of the database to query. If not provided, the database name
                        from the client obejct is used.
        :param branch_name: The name of the branch to query. If not provided, the branch name
                        from the client obejct is used.
        :return: A record as a dictionary or None if it doesn't exist.
        """
        db_name, branch_name = self.db_and_branch_names_from_params(db_name, branch_name)
        result = self.request(
            "GET",
            f"/db/{db_name}:{branch_name}/tables/{table}/data/{id}",
            expect_codes=[404],
        )
        if result.status_code == 404:
            return None
        return result.json()

    @deprecation.deprecated(
        deprecated_in="0.7.0",
        removed_in="1.0",
        current_version=__version__,
        details="client.records().insertRecord(:table) or client.records().insertRecordWithId(:table, :id)",
    )
    def create(
        self,
        table: str,
        record: dict,
        id: str = None,
        db_name: str = None,
        branch_name: str = None,
    ) -> str:
        """
        Please use: client.records().insertRecord(:table) or client.records().insertRecordWithId(:table, :id)
        Create a record in a table. If an ID is not provided, one will be generated.
        If the ID is provided and a record with that ID already exists, an error is returned.

        :param table: The name of the table to query.
        :param id: The ID of the record to create. If not provided, one will be generated.
        :param record: The record to create, as dict.
        :param db_name: The name of the database to query. If not provided, the database name
                        from the client obejct is used.
        :param branch_name: The name of the branch to query. If not provided, the branch name
                        from the client obejct is used.
        :return: The ID of the created record.
        """
        db_name, branch_name = self.db_and_branch_names_from_params(db_name, branch_name)
        if id is not None:
            self.request(
                "PUT",
                f"/db/{db_name}:{branch_name}/tables/{table}/data/{id}",
                params=dict(createOnly=True),
                json=record,
            )
            return id

        result = self.request("POST", f"/db/{db_name}:{branch_name}/tables/{table}/data", json=record)
        return result.json()["id"]

    @deprecation.deprecated(
        deprecated_in="0.7.0",
        removed_in="1.0",
        current_version=__version__,
        details="client.records().upsertRecordWithID(:table, :id)",
    )
    def create_or_update(
        self,
        table: str,
        id: str,
        record: dict,
        db_name: str = None,
        branch_name: str = None,
    ) -> str:
        """
        Create or updated a record in a table. If a record with the same id already
        exists, it will be updated. Only the provided columns in record are replaced, is
        a column is not present explicitely in record, it is not updated.

        :meta public:
        :param table: The name of the table to query.
        :param id: The ID of the record to create or update.
        :param record: The record to create or the keys/values to use to update, as dict.
        :param db_name: The name of the database to query. If not provided, the database name
                        from the client obejct is used.
        :param branch_name: The name of the branch to query. If not provided, the branch name
                        from the client obejct is used.
        :return: The ID of the created or updated record.
        """
        db_name, branch_name = self.db_and_branch_names_from_params(db_name, branch_name)
        result = self.request("POST", f"/db/{db_name}:{branch_name}/tables/{table}/data/{id}", json=record)
        return result.json()["id"]

    @deprecation.deprecated(
        deprecated_in="0.7.0",
        removed_in="1.0",
        current_version=__version__,
        details="client.records().insertRecordWithID(:table, :id)",
    )
    def create_or_replace(
        self,
        table: str,
        id: str,
        record: dict,
        db_name: str = None,
        branch_name: str = None,
    ) -> str:
        """Create or replace a record in a table. If a record with the same id already
        exists, it will be relaced completely.

        :meta public:
        :param table: The name of the table to query.
        :param id: The ID of the record to create or replace.
        :param record: The record to create or replace, as dict.
        :param db_name: The name of the database to query. If not provided, the database name
                        from the client obejct is used.
        :param branch_name: The name of the branch to query. If not provided, the branch name
                        from the client obejct is used.
        :return: The ID of the created or updated record.
        """
        db_name, branch_name = self.db_and_branch_names_from_params(db_name, branch_name)
        result = self.request("PUT", f"/db/{db_name}:{branch_name}/tables/{table}/data/{id}", json=record)
        return result.json()["id"]

    @deprecation.deprecated(
        deprecated_in="0.7.0",
        removed_in="1.0",
        current_version=__version__,
        details="client.records().updateRecordWithID(:table, :id)",
    )
    def update(
        self,
        table: str,
        id: str,
        record: dict,
        ifVersion: Optional[int] = None,
        db_name: str = None,
        branch_name: str = None,
    ) -> Optional[dict]:
        """Updates the record with the given key-value pairs in the record. The columns
        that aren't explicitely provided are left unchanged. If no record with the given
        id exists, None is returned. If the ifVersion condition is not respected, None is
        returned.

        :meta public:
        :param table: The name of the table to query.
        :param id: The ID of the record to update.
        :param record: The key-value pairs to update.
        :param ifVersion: Only perform the update if the version of the record matches this value.
        :param db_name: The name of the database to query. If not provided, the database name
                        from the client obejct is used.
        :param branch_name: The name of the branch to query. If not provided, the branch name
                        from the client obejct is used.
        :return: The updated record.
        """
        db_name, branch_name = self.db_and_branch_names_from_params(db_name, branch_name)
        params = {"columns": "*"}
        if ifVersion is not None:
            params["ifVersion"] = str(ifVersion)
        result = self.request(
            "PATCH",
            f"/db/{db_name}:{branch_name}/tables/{table}/data/{id}",
            params=params,
            json=record,
            expect_codes=[422, 404],
        )
        if result.status_code == 404:
            return None  # TODO: I would prefer to raise here, but there is a backend issue
        if result.status_code == 422:
            return None
        return result.json()

    @deprecation.deprecated(
        deprecated_in="0.7.0",
        removed_in="1.0",
        current_version=__version__,
        details="client.records().deleteRecord(:table, :id)",
    )
    def delete_record(self, table: str, id: str, db_name: str = None, branch_name: str = None) -> Optional[dict]:
        """Deletes the record with the given ID. Returns the record as it was just before
        deletion. If no record with the given ID exists, raises RecordNotFoundException.

        :meta public:
        :param table: The name of the table to query.
        :param id: The ID of the record to delete.
        :param db_name: The name of the database to query. If not provided, the database name
                        from the client obejct is used.
        :param branch_name: The name of the branch to query. If not provided, the branch name
                        from the client obejct is used.
        :return: The deleted record.
        """
        db_name, branch_name = self.db_and_branch_names_from_params(db_name, branch_name)
        params = {"columns": "*"}
        result = self.request(
            "DELETE",
            f"/db/{db_name}:{branch_name}/tables/{table}/data/{id}",
            params=params,
            expect_codes=[404],
        )
        if result.status_code == 404:
            raise RecordNotFoundException(id)
        return result.json()

    @deprecation.deprecated(
        deprecated_in="0.7.0",
        removed_in="1.0",
        current_version=__version__,
        details="client.search_and_filter().searchBranch(:payload)",
    )
    def search(
        self,
        query: str,
        query_params: dict = {},
        db_name: str = None,
        branch_name: str = None,
    ) -> Optional[dict]:
        """This endpoint performs full text search across an entire database branch.

        API docs: https://xata.io/docs/api-reference/db/db_branch_name/search#free-text-search

        :meta public:
        :param query: search string
        :param query_params: more granular search criteria, see API docs for options
        :param db_name: The name of the database to query. If not provided, the database name
                        from the client obejct is used.
        :param branch_name: The name of the branch to query. If not provided, the branch name
                        from the client obejct is used.

        :return set of matching records
        """
        db_name, branch_name = self.db_and_branch_names_from_params(db_name, branch_name)
        query_params["query"] = query.strip()
        result = self.request(
            "POST",
            f"/db/{db_name}:{branch_name}/search",
            json=query_params,
            expect_codes=[200, 400, 403, 404, 500],
        )

        if result.status_code == 400:
            raise BadRequestException(result.status_code, result.json()["message"])
        return result.json()

    @deprecation.deprecated(
        deprecated_in="0.7.0",
        removed_in="1.0",
        current_version=__version__,
        details="client.search_and_filter().searchTable(:payload)",
    )
    def search_table(
        self,
        table_name: str,
        query: str,
        query_params: dict = {},
        db_name: str = None,
        branch_name: str = None,
    ) -> Optional[dict]:
        """Run a free text search operation in a particular table.

        :meta public:
        :param table_name: table to search
        :param query: search string
        :param query_params: more granular search criteria, see API docs for options
        :param db_name: The name of the database to query. If not provided, the database name
                        from the client obejct is used.
        :param branch_name: The name of the branch to query. If not provided, the branch name
                        from the client obejct is used.

        :return set of matching records
        """
        db_name, branch_name = self.db_and_branch_names_from_params(db_name, branch_name)
        query_params["query"] = query.strip()
        result = self.request(
            "POST",
            f"/db/{db_name}:{branch_name}/tables/{table_name}/search",
            json=query_params,
            expect_codes=[200, 400, 403, 404, 500],
        )

        if result.status_code == 400:
            raise BadRequestException(result.status_code, result.json()["message"])
        return result.json()

    def authentication(self) -> Authentication:
        """
        Authentication Namespace
        :return Authentication
        """
        return self._authentication

    def databases(self) -> Databases:
        """
        Databases Namespace
        :return Databases
        """
        return self._databases

    def invites(self) -> Invites:
        """
        Invites Namespace
        :return Invites
        """
        return self._invites

    def users(self) -> Users:
        """
        Users Namespace
        :return Users
        """
        return self._users

    def workspaces(self) -> Workspaces:
        """
        Workspaces Namespace
        :return Workspaces
        """
        return self._workspaces

    def branch(self) -> Branch:
        """
        Branch Namespace
        :return Branch
        """
        return self._branch

    def migrations(self) -> Migrations:
        """
        Migrations Namespace
        :return Migrations
        """
        return self._migrations

    def records(self) -> Records:
        """
        Records Namespace
        :return Records
        """
        return self._records

    def search_and_filter(self) -> Search_and_filter:
        """
        Search_and_Filter Namespace
        :return Search_and_filter
        """
        return self._search_and_filter

    def data(self) -> Search_and_filter:
        """
        Shorter alias for Search_and_Filter
        :return Search_and_filter
        """
        return self._search_and_filter

    def table(self) -> Table:
        """
        Table Namespace
        :return Table
        """
        return self._table
