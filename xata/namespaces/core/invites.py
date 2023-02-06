# ------------------------------------------------------- #
# Invites
# Manage user invites.
# Specification: core:v1.0
# Base URL: https://api.xata.io
# ------------------------------------------------------- #

from requests import Response
from ..namespace import Namespace

class Invites(Namespace):

    base_url = "https://api.xata.io"
    scope    = "core"

    def inviteWorkspaceMember(self, workspace_id: str, payload: dict) -> Response:
       """
       Invite some user to join the workspace with the given role
       path: /workspaces/{workspace_id}/invites
       method: POST

       :param workspace_id: str Workspace ID
       :param payload: dict Request Body
       """
       url_path = f"/workspaces/{workspace_id}/invites"
       headers = {"content-type": "application/json"}
       return self.request("POST", url_path, payload, headers)


    def cancelWorkspaceMemberInvite(self, workspace_id: str, invite_id: str) -> Response:
       """
       This operation provides a way to cancel invites by deleting them. Already accepted invites cannot be deleted.
       path: /workspaces/{workspace_id}/invites/{invite_id}
       method: DELETE

       :param workspace_id: str Workspace ID
       :param invite_id: str Invite identifier
       """
       url_path = f"/workspaces/{workspace_id}/invites/{invite_id}"
       return self.request("DELETE", url_path)


    def acceptWorkspaceMemberInvite(self, workspace_id: str, invite_key: str) -> Response:
       """
       Accept the invitation to join a workspace. If the operation succeeds the user will be a member of the workspace

       path: /workspaces/{workspace_id}/invites/{invite_key}/accept
       method: POST

       :param workspace_id: str Workspace ID
       :param invite_key: str Invite Key (secret) for the invited user
       """
       url_path = f"/workspaces/{workspace_id}/invites/{invite_key}/accept"
       return self.request("POST", url_path)


    def resendWorkspaceMemberInvite(self, workspace_id: str, invite_id: str) -> Response:
       """
       This operation provides a way to resend an Invite notification. Invite notifications can only be sent for Invites not yet accepted.
       path: /workspaces/{workspace_id}/invites/{invite_id}/resend
       method: POST

       :param workspace_id: str Workspace ID
       :param invite_id: str Invite identifier
       """
       url_path = f"/workspaces/{workspace_id}/invites/{invite_id}/resend"
       return self.request("POST", url_path)

