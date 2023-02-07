# ------------------------------------------------------- #
# Records
# Record access API.
# Specification: workspace:v1.0
# Base URL: https://{workspaceId}.{regionId}.xata.sh
# ------------------------------------------------------- #

from requests import Response

from xata.namespace import Namespace


class Records(Namespace):

    base_url = "https://{workspaceId}.{regionId}.xata.sh"
    scope = "workspace"

    def branchTransaction(self, db_branch_name: str, payload: dict) -> Response:
        """
        Execute a transaction on a branch
        path: /db/{db_branch_name}/transaction
        method: POST

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`.

        :param payload: dict Request Body
        :return Response
        """
        url_path = f"/db/{db_branch_name}/transaction"
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, payload, headers)

    def insertRecord(
        self, db_branch_name: str, table_name: str, columns: list, payload: dict
    ) -> Response:
        """
        Insert a new Record into the Table
        path: /db/{db_branch_name}/tables/{table_name}/data
        method: POST

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`.

        :param table_name: str The Table name
        :param columns: list Column filters
        :param payload: dict Request Body
        :return Response
        """
        url_path = f"/db/{db_branch_name}/tables/{table_name}/data"
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, payload, headers)

    def getRecord(
        self, db_branch_name: str, table_name: str, record_id: str, columns: list
    ) -> Response:
        """
        Retrieve record by ID
        path: /db/{db_branch_name}/tables/{table_name}/data/{record_id}
        method: GET

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`.

        :param table_name: str The Table name
        :param record_id: str The Record name
        :param columns: list Column filters
        :return Response
        """
        url_path = f"/db/{db_branch_name}/tables/{table_name}/data/{record_id}"
        return self.request("GET", url_path)

    def insertRecordWithID(
        self,
        db_branch_name: str,
        table_name: str,
        record_id: str,
        columns: list,
        payload: dict,
    ) -> Response:
        """
        By default, IDs are auto-generated when data is insterted into Xata. Sending a request to this endpoint allows us to insert a record with a pre-existing ID, bypassing the default automatic ID generation.
        path: /db/{db_branch_name}/tables/{table_name}/data/{record_id}
        method: PUT

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`.

        :param table_name: str The Table name
        :param record_id: str The Record name
        :param columns: list Column filters
        :param payload: dict Request Body
        :return Response
        """
        url_path = f"/db/{db_branch_name}/tables/{table_name}/data/{record_id}"
        headers = {"content-type": "application/json"}
        return self.request("PUT", url_path, payload, headers)

    def upsertRecordWithID(
        self,
        db_branch_name: str,
        table_name: str,
        record_id: str,
        columns: list,
        payload: dict,
    ) -> Response:
        """
        Upsert record with ID
        path: /db/{db_branch_name}/tables/{table_name}/data/{record_id}
        method: POST

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`.

        :param table_name: str The Table name
        :param record_id: str The Record name
        :param columns: list Column filters
        :param payload: dict Request Body
        :return Response
        """
        url_path = f"/db/{db_branch_name}/tables/{table_name}/data/{record_id}"
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, payload, headers)

    def deleteRecord(
        self, db_branch_name: str, table_name: str, record_id: str, columns: list
    ) -> Response:
        """
        Delete record from table
        path: /db/{db_branch_name}/tables/{table_name}/data/{record_id}
        method: DELETE

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`.

        :param table_name: str The Table name
        :param record_id: str The Record name
        :param columns: list Column filters
        :return Response
        """
        url_path = f"/db/{db_branch_name}/tables/{table_name}/data/{record_id}"
        return self.request("DELETE", url_path)

    def updateRecordWithID(
        self,
        db_branch_name: str,
        table_name: str,
        record_id: str,
        columns: list,
        payload: dict,
    ) -> Response:
        """
        Update record with ID
        path: /db/{db_branch_name}/tables/{table_name}/data/{record_id}
        method: PATCH

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`.

        :param table_name: str The Table name
        :param record_id: str The Record name
        :param columns: list Column filters
        :param payload: dict Request Body
        :return Response
        """
        url_path = f"/db/{db_branch_name}/tables/{table_name}/data/{record_id}"
        headers = {"content-type": "application/json"}
        return self.request("PATCH", url_path, payload, headers)

    def bulkInsertTableRecords(
        self, db_branch_name: str, table_name: str, columns: list, payload: dict
    ) -> Response:
        """
        Bulk insert records
        path: /db/{db_branch_name}/tables/{table_name}/bulk
        method: POST

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`.

        :param table_name: str The Table name
        :param columns: list Column filters
        :param payload: dict Request Body
        :return Response
        """
        url_path = f"/db/{db_branch_name}/tables/{table_name}/bulk"
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, payload, headers)
