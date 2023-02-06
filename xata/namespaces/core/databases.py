# ------------------------------------------------------- #
# Databases
# Workspace databases management.
# Specification: core:v1.0
# Base URL: https://api.xata.io
# ------------------------------------------------------- #

from requests import Response
from ..namespace import Namespace

class Databases(Namespace):

    base_url = "https://api.xata.io"
    scope    = "core"

    def getDatabaseList(self, workspace_id: str) -> Response:
       """
       List all databases available in your Workspace.
       path: /workspaces/{workspace_id}/dbs
       method: GET

       :param workspace_id: str Workspace ID
       """
       url_path = f"/workspaces/{workspace_id}/dbs"
       return self.request("GET", url_path)


    def getDatabaseMetadata(self, workspace_id: str, db_name: str) -> Response:
       """
       Retrieve metadata of the given database
       path: /workspaces/{workspace_id}/dbs/{db_name}
       method: GET

       :param workspace_id: str Workspace ID
       :param db_name: str The Database Name
       """
       url_path = f"/workspaces/{workspace_id}/dbs/{db_name}"
       return self.request("GET", url_path)


    def createDatabase(self, workspace_id: str, db_name: str, payload: dict) -> Response:
       """
       Create Database with identifier name
       path: /workspaces/{workspace_id}/dbs/{db_name}
       method: PUT

       :param workspace_id: str Workspace ID
       :param db_name: str The Database Name
       :param payload: dict Request Body
       """
       url_path = f"/workspaces/{workspace_id}/dbs/{db_name}"
       headers = {"content-type": "application/json"}
       return self.request("PUT", url_path, payload, headers)


    def deleteDatabase(self, workspace_id: str, db_name: str) -> Response:
       """
       Delete a database and all of its branches and tables permanently.
       path: /workspaces/{workspace_id}/dbs/{db_name}
       method: DELETE

       :param workspace_id: str Workspace ID
       :param db_name: str The Database Name
       """
       url_path = f"/workspaces/{workspace_id}/dbs/{db_name}"
       return self.request("DELETE", url_path)


    def listRegions(self, workspace_id: str) -> Response:
       """
       List regions available to create a database on
       path: /workspaces/{workspace_id}/regions
       method: GET

       :param workspace_id: str Workspace ID
       """
       url_path = f"/workspaces/{workspace_id}/regions"
       return self.request("GET", url_path)

