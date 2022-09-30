import os
from typing import Literal
from urllib.request import Request
import requests
import json
from dotenv import dotenv_values
from urllib.parse import urljoin

PERSONAL_API_KEY_LOCATION="~/.config/xata/key"
DEFAULT_BASE_URL_DOMAIN="xata.sh"
CONFIG_LOCATION=".xatarc"

ApiKeyLocation = Literal["env", "dotenv", "profile", "parameter"]
WorkspaceIdLocation = Literal["parameter", "env", "config"]

class UnauthorizedException(Exception):
    pass

class RateLimitException(Exception):
    pass

class BadRequestException(Exception):
    pass

class ServerErrorException(Exception):
    pass

class XataClient:
    configRead: bool = False
    config = None
    def __init__(self,
             api_key: str = None,
             base_url_domain: str = DEFAULT_BASE_URL_DOMAIN,
             workspace_id: str = None):

        if api_key is None:
            self.api_key, self.api_key_location = self.getApiKey()
        else:
            self.api_key, self.api_key_location = api_key, "parameter"
        if workspace_id is None:
            self.workspace_id, self.workspace_id_location = self.getWorkspaceId()
        else:
            self.workspace_id = workspace_id, "parameter"
        self.base_url = f"https://{self.workspace_id}.{base_url_domain}"

        self.dbName = self.getDatabaseNameIfConfigured()
        self.branchName = self.getBranchNameIfConfigured()
        # print (f"API key: {self.api_key}, location: {self.api_key_location}, workspaceId: {self.workspace_id}")

    def getApiKey(self) -> tuple[str, ApiKeyLocation]:
        if os.environ.get('XATA_API_KEY') is not None:
            return os.environ.get('XATA_API_KEY'), "env" 

        envVals = dotenv_values(".env")        
        if envVals.get('XATA_API_KEY') is not None:
            return envVals.get('XATA_API_KEY'), "dotenv"

        if os.path.isfile(os.path.expanduser(PERSONAL_API_KEY_LOCATION)):
            with open(os.path.expanduser(PERSONAL_API_KEY_LOCATION), 'r') as f:
                return f.read().strip(), "profile"

        raise Exception(f"No API key found. Searched in `XATA_API_KEY` env, `{PERSONAL_API_KEY_LOCATION}`, and `{os.path.abspath('.env')}`")

    def getWorkspaceId(self) -> tuple[str, WorkspaceIdLocation]:
        if os.environ.get('XATA_WORKSPACE_ID') is not None:
            return os.environ.get('XATA_WORKSPACE_ID'), "env" 

        self.ensureConfigRead()
        if self.config is not None and self.config.get("databaseURL"):
            workspaceID, _ = self.parseDatabaseUrl(self.config.get("databaseURL"))
            return workspaceID, "config"
        raise Exception(f"No workspace ID found. Searched in `XATA_WORKSPACE_ID` env, `{PERSONAL_API_KEY_LOCATION}`, and `{os.path.abspath('.env')}`")

    def getDatabaseNameIfConfigured(self) -> str:
        self.ensureConfigRead()
        if self.config is not None and self.config.get("databaseURL"):
            _, dbName = self.parseDatabaseUrl(self.config.get("databaseURL"))
            return dbName
        return None

    def getBranchNameIfConfigured(self) -> str:
        # TODO: resolve branch name by the current git branch
        return os.environ.get('XATA_BRANCH')

    def request(self, method, urlPath, headers={}, **kwargs):
        headers['Authorization'] = f"Bearer {self.api_key}"
        url = urljoin(self.base_url, urlPath)
        resp = requests.request(method, url, headers=headers, **kwargs)
        if resp.status_code > 299:
            if resp.status_code == 401:
                raise UnauthorizedException(f"Unauthorized: {resp.json()} API key location: {self.api_key_location}")
            elif resp.status_code == 429:
                raise RateLimitException(f"Rate limited: {resp.json()}")
            elif resp.status_code >= 399 and resp.status_code < 500:
                raise BadRequestException(f"Bad request: {resp.json()}")
            elif resp.status_code >= 500:
                raise ServerErrorException(f"Server error: {resp.text}")
            raise Exception(f"{resp.status_code} {resp.text}")
        return resp

    def ensureConfigRead(self) -> bool:
        if self.configRead:
            return False
        if os.path.isfile(CONFIG_LOCATION):
            with open(CONFIG_LOCATION, 'r') as f:
                self.config = json.load(f)
        self.configRead = True
        return True

    def parseDatabaseUrl(self, databaseURL: str) -> tuple[str, str]:
        (_, _, host, _, db) = databaseURL.split("/")
        if host == "":
            raise Exception("Invalid database URL")
        parts = host.split(".")
        workspaceId = parts[0]
        return workspaceId, db

    def get(self, urlPath, headers={}, **kwargs):
        return self.request("GET", urlPath, headers=headers, **kwargs)

    def post(self, urlPath, headers={}, **kwargs):
        return self.request("POST", urlPath, headers=headers, **kwargs)

    def put(self, urlPath, headers={}, **kwargs):
        return self.request("PUT", urlPath, headers=headers, **kwargs)

    def delete(self, urlPath, headers={}, **kwargs):
        return self.request("DELETE", urlPath, headers=headers, **kwargs)

    def requestBodyFromParams(self,         
        columns: list[str] = None,
        filter: dict = None,
        sort: dict = None, 
        page: dict = None) -> dict:

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

    def dbAndBranchNamesFromParams(self, dbName, branchName) -> tuple[str, str]:
        dbName = dbName or self.dbName
        branchName = branchName or self.branchName
        if dbName is None:
            raise Exception("Database name is not configured. Please set it via `xata init` or pass it as a parameter.")
        if branchName is None:
            raise Exception("Branch name is not configured. Please set it in the `XATA_BRANCH` env var or pass it as a parameter.")
        return dbName, branchName

    """
    Query a table.

    :param table: The name of the table to query.
    :param dbName: The name of the database to query. If not provided, the database name 
                   from the client obejct is used.
    :param branchName: The name of the branch to query. If not provided, the branch name
                        from the client obejct is used.
    :param columns: A list of column names to return. If not provided, all columns are returned.
    :param filter: A filter expression to apply to the query.
    :param sort: A sort expression to apply to the query.
    :param page: A page expression to apply to the query.
    :return: A page of results.
    """
    def query(self, 
        table: str, 
        dbName: str = None,
        branchName: str = None,
        columns: list[str] = None,
        filter: dict = None,
        sort: dict = None, 
        page: dict = None) -> dict:

        dbName, branchName = self.dbAndBranchNamesFromParams(dbName, branchName)
        body = self.requestBodyFromParams(columns, filter, sort, page)
        result = self.post(f"/db/{dbName}:{branchName}/tables/{table}/query", json=body)
        return result.json()

    """
    Get one record from a table.

    :param table: The name of the table to query.
    :param dbName: The name of the database to query. If not provided, the database name
                     from the client obejct is used.
    :param branchName: The name of the branch to query. If not provided, the branch name
                        from the client obejct is used.
    :param columns: A list of column names to return. If not provided, all columns are returned.
    :param filter: A filter expression to apply to the query.
    :param sort: A sort expression to apply to the query.
    :return: A record as a dictionary.
    """
    def getOne(self, table,
        dbName = None,
        branchName = None,
        columns = None,
        filter = None,
        sort = None) -> dict:

        page = {"size": 1}
        dbName, branchName = self.dbAndBranchNamesFromParams(dbName, branchName)
        body = self.requestBodyFromParams(columns, filter, sort, page)
        result = self.post(f"/db/{dbName}:{branchName}/tables/{table}/query", json=body)
        data = result.json()
        if len(data.get("records", [])) == 0:
            return None
        return data.get("records")[0]
    
    """
    Create a record in a table. If an ID is not provided, one will be generated.
    If the ID is provided and a record with that ID already exists, an error is returned.

    :param table: The name of the table to query.
    :param dbName: The name of the database to query. If not provided, the database name
                     from the client obejct is used.
    :param branchName: The name of the branch to query. If not provided, the branch name
                        from the client obejct is used.
    :param id: The ID of the record to create. If not provided, one will be generated.
    :param record: The record to create, as dict.
    :return: The ID of the created record.
    """
    def create(self, table, 
        dbName: str = None,
        branchName: str = None,
        id: str = None,
        record: dict = None) -> str:

        dbName, branchName = self.dbAndBranchNamesFromParams(dbName, branchName)
        if id is not None:
            self.put(f"/db/{dbName}:{branchName}/tables/{table}/records/{id}", json=record)
            return id

        result = self.post(f"/db/{dbName}:{branchName}/tables/{table}/data", json=record)
        return result.json()["id"]