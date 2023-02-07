# ------------------------------------------------------- #
# Workspaces
# Workspaces management
# Specification: core:v1.0
# Base URL: https://api.xata.io
# ------------------------------------------------------- #

from requests import Response

from xata.namespace import Namespace


class Workspaces(Namespace):

    base_url = "https://api.xata.io"
    scope = "core"

    def getWorkspacesList(
        self,
    ) -> Response:
        """
        Retrieve the list of workspaces the user belongs to
        path: /workspaces
        method: GET

        :return Response
        """
        url_path = "/workspaces"
        return self.request("GET", url_path)

    def createWorkspace(self, payload: dict) -> Response:
        """
        Creates a new workspace with the user requesting it as its single owner.
        path: /workspaces
        method: POST

        :param payload: dict Request Body
        :return Response
        """
        url_path = "/workspaces"
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, payload, headers)

    def getWorkspace(self, workspace_id: str) -> Response:
        """
        Retrieve workspace info from a workspace ID
        path: /workspaces/{workspace_id}
        method: GET

        :param workspace_id: str Workspace ID
        :return Response
        """
        url_path = f"/workspaces/{workspace_id}"
        return self.request("GET", url_path)

    def updateWorkspace(self, workspace_id: str, payload: dict) -> Response:
        """
        Update workspace info
        path: /workspaces/{workspace_id}
        method: PUT

        :param workspace_id: str Workspace ID
        :param payload: dict Request Body
        :return Response
        """
        url_path = f"/workspaces/{workspace_id}"
        headers = {"content-type": "application/json"}
        return self.request("PUT", url_path, payload, headers)

    def deleteWorkspace(self, workspace_id: str) -> Response:
        """
        Delete the workspace with the provided ID
        path: /workspaces/{workspace_id}
        method: DELETE

        :param workspace_id: str Workspace ID
        :return Response
        """
        url_path = f"/workspaces/{workspace_id}"
        return self.request("DELETE", url_path)

    def getWorkspaceMembersList(self, workspace_id: str) -> Response:
        """
        Retrieve the list of members of the given workspace
        path: /workspaces/{workspace_id}/members
        method: GET

        :param workspace_id: str Workspace ID
        :return Response
        """
        url_path = f"/workspaces/{workspace_id}/members"
        return self.request("GET", url_path)

    def updateWorkspaceMemberRole(
        self, workspace_id: str, user_id: str, payload: dict
    ) -> Response:
        """
        Update a workspace member role. Workspaces must always have at least one owner, so this operation will fail if trying to remove owner role from the last owner in the workspace.

        path: /workspaces/{workspace_id}/members/{user_id}
        method: PUT

        :param workspace_id: str Workspace ID
        :param user_id: str UserID
        :param payload: dict Request Body
        :return Response
        """
        url_path = f"/workspaces/{workspace_id}/members/{user_id}"
        headers = {"content-type": "application/json"}
        return self.request("PUT", url_path, payload, headers)

    def removeWorkspaceMember(self, workspace_id: str, user_id: str) -> Response:
        """
        Remove the member from the workspace
        path: /workspaces/{workspace_id}/members/{user_id}
        method: DELETE

        :param workspace_id: str Workspace ID
        :param user_id: str UserID
        :return Response
        """
        url_path = f"/workspaces/{workspace_id}/members/{user_id}"
        return self.request("DELETE", url_path)
