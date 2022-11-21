import json
import os
from typing import Literal, Optional
from urllib.parse import urljoin

import requests
from dotenv import dotenv_values

from .errors import (
    BadRequestException,
    RateLimitException,
    RecordNotFoundException,
    UnauthorizedException,
)

PERSONAL_API_KEY_LOCATION = "~/.config/xata/key"
DEFAULT_BASE_URL_DOMAIN = "xata.sh"
DEFAULT_CONTROL_PLANE_DOMAIN = "api.xata.io"
DEFAULT_REGION = "us-east-1"
CONFIG_LOCATION = ".xatarc"

ApiKeyLocation = Literal["env", "dotenv", "profile", "parameter"]
WorkspaceIdLocation = Literal["parameter", "env", "config"]


class ServerErrorException(Exception):
    status_code: int
    message: str

    def __init__(self, status_code, message):
        self.status_code = status_code
        self.message = message
        super().__init__(message)

    def __str__(self) -> str:
        return f"Server error: {self.status_code} {self.message}"


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
    :param branch_name: The branch name to use.
    :param base_url_domain: The domain to use for the base URL. Defaults to xata.sh.
    :param control_plane_domain: The domain to use for the control plane. Defaults to api.xata.io.
    """

    configRead: bool = False
    config = None

    def __init__(
        self,
        api_key: str = None,
        base_url_domain: str = DEFAULT_BASE_URL_DOMAIN,
        control_plane_domain: str = DEFAULT_CONTROL_PLANE_DOMAIN,
        region: str = DEFAULT_REGION,
        workspace_id: str = None,
        db_name: str = None,
        db_url: str = None,
        branch_name: str = None,
    ):
        """Constructor for the XataClient."""
        if db_url is not None:
            if workspace_id is not None or db_name is not None:
                raise Exception(
                    "Cannot specify both db_url and workspace_id/region/db_name"
                )
            workspace_id, region, db_name = self.parse_database_url(db_url)

        if api_key is None:
            self.api_key, self.api_key_location = self.get_api_key()
        else:
            self.api_key, self.api_key_location = api_key, "parameter"
        if workspace_id is None:
            (
                self.workspace_id,
                self.region,
                self.workspace_id_location,
            ) = self.get_workspace_id()
        else:
            self.workspace_id = workspace_id
            self.workspace_id_location = "parameter"
            self.region = region
        self.base_url = f"https://{self.workspace_id}.{self.region}.{base_url_domain}"
        self.control_plane_url = (
            f"https://{control_plane_domain}/workspaces/{self.workspace_id}/"
        )

        self.db_name = (
            self.get_database_name_if_configured() if db_name is None else db_name
        )
        self.branch_name = (
            self.get_branch_name_if_configured() if branch_name is None else branch_name
        )
        # print (
        #   f"API key: {self.api_key}, "
        #   f"location: {self.api_key_location}, "
        #   f"workspaceId: {self.workspace_id}, "
        #   f"region: {self.region}, "
        #   f"dbName: {self.db_name}, "
        #   f"branchName: {self.branch_name}"
        # )

    def get_api_key(self) -> tuple[str, ApiKeyLocation]:
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

    def get_workspace_id(self) -> tuple[str, str, WorkspaceIdLocation]:
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
            workspaceID, region, _ = self.parse_database_url(
                self.config.get("databaseURL")
            )
            return workspaceID, region, "config"
        raise Exception(
            f"No workspace ID found. Searched in `XATA_WORKSPACE_ID` env, "
            f"`{PERSONAL_API_KEY_LOCATION}`, and `{os.path.abspath('.env')}`"
        )

    def get_database_name_if_configured(self) -> str:
        self.ensure_config_read()
        if self.config is not None and self.config.get("databaseURL"):
            _, _, db_name = self.parse_database_url(self.config.get("databaseURL"))
            return db_name
        return None

    def get_branch_name_if_configured(self) -> str:
        # TODO: resolve branch name by the current git branch
        return os.environ.get("XATA_BRANCH")

    def request(self, method, urlPath, cp=False, headers={}, expect_codes=[], **kwargs):
        headers["Authorization"] = f"Bearer {self.api_key}"
        base_url = self.base_url if not cp else self.control_plane_url
        url = urljoin(base_url, urlPath.lstrip("/"))
        resp = requests.request(method, url, headers=headers, **kwargs)
        if resp.status_code > 299:
            if resp.status_code in expect_codes:
                return resp
            if resp.status_code == 401:
                raise UnauthorizedException(
                    f"Unauthorized: {resp.json()} API key location: {self.api_key_location}"
                )
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

    def parse_database_url(self, databaseURL: str) -> tuple[str, str, str]:
        (_, _, host, _, db) = databaseURL.split("/")
        if host == "":
            raise Exception("Invalid database URL")
        parts = host.split(".")
        workspaceId = parts[0]
        region = parts[1]
        return workspaceId, region, db

    def get(self, urlPath, headers={}, **kwargs):
        """Send a GET request to the Xata API. This is a wrapper around
        the `requests` library and accepts the same parameters as the `get`
        method of the `requests` library.
        """

        return self.request("GET", urlPath, headers=headers, **kwargs)

    def post(self, urlPath, headers={}, **kwargs):
        """Send a POST request to the Xata API. This is a wrapper around
        the `requests` library and accepts the same parameters as the `post`
        method of the `requests` library.
        """
        return self.request("POST", urlPath, headers=headers, **kwargs)

    def put(self, urlPath, headers={}, **kwargs):
        """Send a PUT request to the Xata API. This is a wrapper around
        the `requests` library and accepts the same parameters as the `put`
        method of the `requests` library.
        """
        return self.request("PUT", urlPath, headers=headers, **kwargs)

    def delete(self, urlPath, headers={}, **kwargs):
        """Send a DELETE request to the Xata API. This is a wrapper around
        the `requests` library and accepts the same parameters as the `delete`
        method of the `requests` library.
        """
        return self.request("DELETE", urlPath, headers=headers, **kwargs)

    def patch(self, urlPath, headers={}, **kwargs):
        """Send a PATCH request to the Xata API. This is a wrapper around
        the `requests` library and accepts the same parameters as the `patch`
        method of the `requests` library.
        """
        return self.request("PATCH", urlPath, headers=headers, **kwargs)

    def request_body_from_params(
        self,
        columns: list[str] = None,
        filter: dict = None,
        sort: dict = None,
        page: dict = None,
    ) -> dict:

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

    def db_and_branch_names_from_params(self, db_name, branch_name) -> tuple[str, str]:
        db_name = db_name or self.db_name
        branch_name = branch_name or self.branch_name
        if db_name is None:
            raise Exception(
                "Database name is not configured. Please set it via `xata init` or pass it as a parameter."
            )
        if branch_name is None:
            raise Exception(
                "Branch name is not configured. Please set it in the `XATA_BRANCH` env var or pass it as a parameter."
            )
        return db_name, branch_name

    def set_db_and_branch_names(self, db_name: str = None, branch_name: str = None):
        if db_name is not None:
            self.db_name = db_name

        if branch_name is not None:
            self.branch_name = branch_name

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
        """Query a table.

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

        db_name, branch_name = self.db_and_branch_names_from_params(
            db_name, branch_name
        )
        body = self.request_body_from_params(columns, filter, sort, page)
        result = self.post(
            f"/db/{db_name}:{branch_name}/tables/{table}/query", json=body
        )
        return result.json()

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
        db_name, branch_name = self.db_and_branch_names_from_params(
            db_name, branch_name
        )
        body = self.request_body_from_params(columns, filter, sort, page)
        result = self.post(
            f"/db/{db_name}:{branch_name}/tables/{table}/query", json=body
        )
        data = result.json()
        if len(data.get("records", [])) == 0:
            return None
        return data.get("records")[0]

    def get_by_id(
        self,
        table: str,
        id: str,
        db_name: str = None,
        branch_name: str = None,
    ) -> Optional[dict]:
        """Get a specific record by its ID. Returns None if an record with that ID
        doesn't exist.

        :meta public:
        :param table: The name of the table to query.
        :param id: The ID of the record to get.
        :param db_name: The name of the database to query. If not provided, the database name
                        from the client obejct is used.
        :param branch_name: The name of the branch to query. If not provided, the branch name
                            from the client obejct is used.
        :return: A record as a dictionary or None if it doesn't exist.
        """
        db_name, branch_name = self.db_and_branch_names_from_params(
            db_name, branch_name
        )
        result = self.get(
            f"/db/{db_name}:{branch_name}/tables/{table}/data/{id}", expect_codes=[404]
        )
        if result.status_code == 404:
            return None
        return result.json()

    def create(
        self,
        table: str,
        record: dict,
        id: str = None,
        db_name: str = None,
        branch_name: str = None,
    ) -> str:
        """Create a record in a table. If an ID is not provided, one will be generated.
        If the ID is provided and a record with that ID already exists, an error is returned.

        :meta public:
        :param table: The name of the table to query.
        :param id: The ID of the record to create. If not provided, one will be generated.
        :param record: The record to create, as dict.
        :param db_name: The name of the database to query. If not provided, the database name
                        from the client obejct is used.
        :param branch_name: The name of the branch to query. If not provided, the branch name
                            from the client obejct is used.
        :return: The ID of the created record.
        """

        db_name, branch_name = self.db_and_branch_names_from_params(
            db_name, branch_name
        )
        if id is not None:
            self.put(
                f"/db/{db_name}:{branch_name}/tables/{table}/data/{id}",
                params=dict(createOnly=True),
                json=record,
            )
            return id

        result = self.post(
            f"/db/{db_name}:{branch_name}/tables/{table}/data", json=record
        )
        return result.json()["id"]

    def create_or_update(
        self,
        table: str,
        id: str,
        record: dict,
        db_name: str = None,
        branch_name: str = None,
    ) -> str:
        """Create or updated a record in a table. If a record with the same id already
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
        db_name, branch_name = self.db_and_branch_names_from_params(
            db_name, branch_name
        )
        result = self.post(
            f"/db/{db_name}:{branch_name}/tables/{table}/data/{id}", json=record
        )
        return result.json()["id"]

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
        db_name, branch_name = self.db_and_branch_names_from_params(
            db_name, branch_name
        )
        result = self.put(
            f"/db/{db_name}:{branch_name}/tables/{table}/data/{id}", json=record
        )
        return result.json()["id"]

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
        db_name, branch_name = self.db_and_branch_names_from_params(
            db_name, branch_name
        )
        params = {"columns": "*"}
        if ifVersion is not None:
            params["ifVersion"] = str(ifVersion)
        result = self.patch(
            f"/db/{db_name}:{branch_name}/tables/{table}/data/{id}",
            params=params,
            json=record,
            expect_codes=[422, 404],
        )
        if result.status_code == 404:
            return (
                None  # TODO: I would prefer to raise here, but there is a backend issue
            )
        if result.status_code == 422:
            return None
        return result.json()

    def delete_record(
        self, table: str, id: str, db_name: str = None, branch_name: str = None
    ) -> Optional[dict]:
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
        db_name, branch_name = self.db_and_branch_names_from_params(
            db_name, branch_name
        )
        params = {"columns": "*"}
        result = self.delete(
            f"/db/{db_name}:{branch_name}/tables/{table}/data/{id}",
            params=params,
            expect_codes=[404],
        )
        if result.status_code == 404:
            raise RecordNotFoundException(id)
        return result.json()
