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
from typing import Literal

from dotenv import dotenv_values

from .api.authentication import Authentication
from .api.branch import Branch
from .api.databases import Databases
from .api.files import Files
from .api.invites import Invites
from .api.migrations import Migrations
from .api.records import Records
from .api.search_and_filter import SearchAndFilter
from .api.sql import Sql
from .api.table import Table
from .api.users import Users
from .api.workspaces import Workspaces

# TODO this is a manual task, to keep in sync with pyproject.toml
# could/should be automated to keep in sync
__version__ = "1.2.1"

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

    config_read: bool = False
    config = None

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

        self.db_name = self.get_database_name_if_configured() if db_name is None else db_name
        self.branch_name = self.get_branch_name_if_configured() if branch_name is None else branch_name

        self.domain_core = domain_core
        self.domain_workspace = domain_workspace

        # init default headers
        self.headers = {
            "authorization": f"Bearer {self.api_key}",
            "user-agent": f"xataio/xata-py:{__version__}",
            "connection": "keep-alive",
            "x-xata-client-id": str(uuid.uuid4()),
            "x-xata-session-id": str(uuid.uuid4()),
            # the format is key1=value; key2=value (with spaces and no trailing ;)
            "x-xata-agent": f"client=PY_SDK; version={__version__}",
        }

        # init namespaces
        self._authentication = Authentication(self)
        self._branch = Branch(self)
        self._search_and_filter = SearchAndFilter(self)
        self._databases = Databases(self)
        self._files = Files(self)
        self._invites = Invites(self)
        self._migrations = Migrations(self)
        self._records = Records(self)
        self._sql = Sql(self)
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

    def get_database_name(self) -> str:
        return self.get_config()["dbName"]

    def get_branch_name(self) -> str:
        return self.get_config()["branchName"]

    def get_region(self) -> str:
        return self.get_config()["region"]

    def get_workspace_id(self) -> str:
        return self.get_config()["workspaceId"]

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
        :returns bool
        """
        name = name.lower().strip()
        if name not in self.headers:
            return False
        del self.headers[name]
        return True

    def _get_api_key(self) -> tuple[str, ApiKeyLocation]:
        if os.environ.get("XATA_API_KEY") is not None:
            return os.environ.get("XATA_API_KEY"), "env"

        env_vals = dotenv_values(".env")
        if env_vals.get("XATA_API_KEY") is not None:
            return env_vals.get("XATA_API_KEY"), "dotenv"

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

        env_vals = dotenv_values(".env")
        if env_vals.get("XATA_WORKSPACE_ID") is not None:
            return (
                env_vals.get("XATA_WORKSPACE_ID"),
                env_vals.get("XATA_REGION", DEFAULT_REGION),
                "dotenv",
            )

        self.ensure_config_read()
        if self.config is not None and self.config.get("databaseURL"):
            workspace_id, region, _, _, _ = self._parse_database_url(self.config.get("databaseURL"))
            return workspace_id, region, "config"
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

        :returns str
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

    def ensure_config_read(self) -> bool:
        if self.config_read:
            return False
        if os.path.isfile(CONFIG_LOCATION):
            with open(CONFIG_LOCATION, "r") as f:
                self.config = json.load(f)
        self.config_read = True
        return True

    def _parse_database_url(self, database_url: str) -> tuple[str, str, str, str, str]:
        """
        Parse Database URL
        Branch name is optional.
        Format: https://{workspace_id}.{region}.xata.sh/db/{db_name}:{branch_name}
        :returns workspace_id, region, db_name, branch_name, domain
        """
        (_, _, host, _, db_branch_name) = database_url.split("/")
        if host == "" or db_branch_name == "":
            raise Exception(
                "Invalid database URL: '%s', expected the format: "
                + "'https://{workspace_id}.{region}.xata.sh/db/{db_name}:{branch_name}'" % database_url
            )
        # split host {workspace_id}.{region}
        host_parts = host.split(".")
        if len(host_parts) < 4:
            raise Exception(
                "Invalid format for workspaceId and region in the URL: '%s', expected the format: "
                + "'https://{workspace_id}.{region}.xata.sh/db/{db_name}:{branch_name}'" % database_url
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

    def authentication(self) -> Authentication:
        """
        :returns Authentication
        """
        return self._authentication

    def databases(self) -> Databases:
        """
        :returns Databases
        """
        return self._databases

    def invites(self) -> Invites:
        """
        :returns Invites
        """
        return self._invites

    def users(self) -> Users:
        """
        :returns Users
        """
        return self._users

    def workspaces(self) -> Workspaces:
        """
        :returns Workspaces
        """
        return self._workspaces

    def branch(self) -> Branch:
        """
        :returns Branch
        """
        return self._branch

    def migrations(self) -> Migrations:
        """
        :returns Migrations
        """
        return self._migrations

    def records(self) -> Records:
        """
        :returns Records
        """
        return self._records

    def search_and_filter(self) -> SearchAndFilter:
        """
        :returns Search_and_filter
        """
        return self._search_and_filter

    def data(self) -> SearchAndFilter:
        """
        Shorter alias for Search_and_Filter
        :returns Search_and_filter
        """
        return self._search_and_filter

    def table(self) -> Table:
        """
        :returns Table
        """
        return self._table

    def files(self) -> Files:
        """
        :returns Files
        """
        return self._files

    def sql(self) -> Sql:
        return self._sql
