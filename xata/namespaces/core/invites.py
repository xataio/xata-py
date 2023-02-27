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

# ------------------------------------------------------- #
# Invites
# Manage user invites.
# Specification: core:v1.0
# Base URL: https://api.xata.io
# ------------------------------------------------------- #

from requests import Response

from xata.namespace import Namespace


class Invites(Namespace):

    base_url = "https://api.xata.io"
    scope = "core"

    def inviteWorkspaceMember(self, workspace_id: str, payload: dict) -> Response:
        """
        Invite some user to join the workspace with the given role

        Path: /workspaces/{workspace_id}/invites
        Method: POST
        Response status codes:
        - 201: Created
        - 400: Bad Request
        - 401: Authentication Error
        - 403: Authentication Error
        - 404: Example response
        - 409: Example response
        - 5XX: Unexpected Error
        Response: application/json

        :param workspace_id: str Workspace ID
        :param payload: dict content

        :return Response
        """
        url_path = f"/workspaces/{workspace_id}/invites"
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, headers)

    def cancelWorkspaceMemberInvite(
        self, workspace_id: str, invite_id: str
    ) -> Response:
        """
        This operation provides a way to cancel invites by deleting them.  Already accepted
        invites cannot be deleted.

        Path: /workspaces/{workspace_id}/invites/{invite_id}
        Method: DELETE
        Response status codes:
        - 204: No Content
        - 400: Bad Request
        - 401: Authentication Error
        - 403: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error

        :param workspace_id: str Workspace ID
        :param invite_id: str Invite identifier

        :return Response
        """
        url_path = f"/workspaces/{workspace_id}/invites/{invite_id}"
        return self.request("DELETE", url_path)

    def updateWorkspaceMemberInvite(
        self, workspace_id: str, invite_id: str, payload: dict
    ) -> Response:
        """
        This operation provides a way to update an existing invite.  Updates are performed in-
        place; they do not change the invite link, the expiry time, nor do they re-notify the
        recipient of the invite.

        Path: /workspaces/{workspace_id}/invites/{invite_id}
        Method: PATCH
        Response status codes:
        - 200: Updated successfully.
        - 400: Bad Request
        - 401: Authentication Error
        - 403: Authentication Error
        - 404: Example response
        - 422: Example response
        - 5XX: Unexpected Error
        Response: application/json

        :param workspace_id: str Workspace ID
        :param invite_id: str Invite identifier
        :param payload: dict content

        :return Response
        """
        url_path = f"/workspaces/{workspace_id}/invites/{invite_id}"
        headers = {"content-type": "application/json"}
        return self.request("PATCH", url_path, headers)

    def acceptWorkspaceMemberInvite(
        self, workspace_id: str, invite_key: str
    ) -> Response:
        """
        Accept the invitation to join a workspace.  If the operation succeeds the user will be a
        member of the workspace

        Path: /workspaces/{workspace_id}/invites/{invite_key}/accept
        Method: POST
        Response status codes:
        - 204: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 403: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error

        :param workspace_id: str Workspace ID
        :param invite_key: str Invite Key (secret) for the invited user

        :return Response
        """
        url_path = f"/workspaces/{workspace_id}/invites/{invite_key}/accept"
        return self.request("POST", url_path)

    def resendWorkspaceMemberInvite(
        self, workspace_id: str, invite_id: str
    ) -> Response:
        """
        This operation provides a way to resend an Invite notification.  Invite notifications can
        only be sent for Invites not yet accepted.

        Path: /workspaces/{workspace_id}/invites/{invite_id}/resend
        Method: POST
        Response status codes:
        - 204: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 403: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error

        :param workspace_id: str Workspace ID
        :param invite_id: str Invite identifier

        :return Response
        """
        url_path = f"/workspaces/{workspace_id}/invites/{invite_id}/resend"
        return self.request("POST", url_path)
