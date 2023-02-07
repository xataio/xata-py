# ------------------------------------------------------- #
# Migrations
# Branch schema migrations and history.
# Specification: workspace:v1.0
# Base URL: https://{workspaceId}.{regionId}.xata.sh
# ------------------------------------------------------- #

from requests import Response

from xata.namespace import Namespace


class Migrations(Namespace):

    base_url = "https://{workspaceId}.{regionId}.xata.sh"
    scope = "workspace"

    def getBranchMigrationHistory(self, db_branch_name: str, payload: dict) -> Response:
        """
        Get branch migration history [deprecated]
        path: /db/{db_branch_name}/migrations
        method: GET

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`.
        :param payload: dict Request Body
        :return Response
        """
        url_path = f"/db/{db_branch_name}/migrations"
        headers = {"content-type": "application/json"}
        return self.request("GET", url_path, payload, headers)

    def getBranchMigrationPlan(self, db_branch_name: str, payload: dict) -> Response:
        """
        Compute a migration plan from a target schema the branch should be migrated too.
        path: /db/{db_branch_name}/migrations/plan
        method: POST

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`.
        :param payload: dict Request Body
        :return Response
        """
        url_path = f"/db/{db_branch_name}/migrations/plan"
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, payload, headers)

    def executeBranchMigrationPlan(
        self, db_branch_name: str, payload: dict
    ) -> Response:
        """
        Apply a migration plan to the branch
        path: /db/{db_branch_name}/migrations/execute
        method: POST

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`.
        :param payload: dict Request Body
        :return Response
        """
        url_path = f"/db/{db_branch_name}/migrations/execute"
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, payload, headers)

    def getBranchSchemaHistory(self, db_branch_name: str, payload: dict) -> Response:
        """
        Query schema history.
        path: /db/{db_branch_name}/schema/history
        method: POST

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`.
        :param payload: dict Request Body
        :return Response
        """
        url_path = f"/db/{db_branch_name}/schema/history"
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, payload, headers)

    def compareBranchWithUserSchema(
        self, db_branch_name: str, payload: dict
    ) -> Response:
        """
        Compare branch with user schema.
        path: /db/{db_branch_name}/schema/compare
        method: POST

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`.
        :param payload: dict Request Body
        :return Response
        """
        url_path = f"/db/{db_branch_name}/schema/compare"
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, payload, headers)

    def compareBranchSchemas(
        self, db_branch_name: str, branch_name: str, payload: dict
    ) -> Response:
        """
        Compare branch schemas.
        path: /db/{db_branch_name}/schema/compare/{branch_name}
        method: POST

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`.
        :param branch_name: str The Database Name
        :param payload: dict Request Body
        :return Response
        """
        url_path = f"/db/{db_branch_name}/schema/compare/{branch_name}"
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, payload, headers)

    def updateBranchSchema(self, db_branch_name: str, payload: dict) -> Response:
        """
        Update Branch schema
        path: /db/{db_branch_name}/schema/update
        method: POST

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`.
        :param payload: dict Request Body
        :return Response
        """
        url_path = f"/db/{db_branch_name}/schema/update"
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, payload, headers)

    def previewBranchSchemaEdit(self, db_branch_name: str, payload: dict) -> Response:
        """
        Preview branch schema edits.
        path: /db/{db_branch_name}/schema/preview
        method: POST

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`.
        :param payload: dict Request Body
        :return Response
        """
        url_path = f"/db/{db_branch_name}/schema/preview"
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, payload, headers)

    def applyBranchSchemaEdit(self, db_branch_name: str, payload: dict) -> Response:
        """
        Apply edit script.
        path: /db/{db_branch_name}/schema/apply
        method: POST

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`.
        :param payload: dict Request Body
        :return Response
        """
        url_path = f"/db/{db_branch_name}/schema/apply"
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, payload, headers)
