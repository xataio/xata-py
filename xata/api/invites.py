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
# ------------------------------------------------------- #

from xata.api_request import ApiRequest
from xata.api_response import ApiResponse


class Invites(ApiRequest):

    scope = "core"

    def new(self, payload: dict, workspace_id: str = None) -> ApiResponse:
        """
        Invite some user to join the workspace with the given role

        Reference: https://xata.io/docs/api-reference/workspaces/workspace_id/invites#invite-a-user-to-join-the-workspace
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

        :param payload: dict content
        :param workspace_id: str = None The workspace identifier. Default: workspace Id from the client.

        :returns ApiResponse
        """
        if workspace_id is None:
            workspace_id = self.client.get_workspace_id()
        url_path = f"/workspaces/{workspace_id}/invites"
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, headers, payload)

    def cancel(self, invite_id: str, workspace_id: str = None) -> ApiResponse:
        """
        This operation provides a way to cancel invites by deleting them. Already accepted invites cannot be deleted.

        Reference: https://xata.io/docs/api-reference/workspaces/workspace_id/invites/invite_id#deletes-an-invite
        Path: /workspaces/{workspace_id}/invites/{invite_id}
        Method: DELETE
        Response status codes:
        - 204: No Content
        - 400: Bad Request
        - 401: Authentication Error
        - 403: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error

        :param invite_id: str Invite identifier
        :param workspace_id: str = None The workspace identifier. Default: workspace Id from the client.

        :returns ApiResponse
        """
        if workspace_id is None:
            workspace_id = self.client.get_workspace_id()
        url_path = f"/workspaces/{workspace_id}/invites/{invite_id}"
        return self.request("DELETE", url_path)

    def update(self, invite_id: str, payload: dict, workspace_id: str = None) -> ApiResponse:
        """
        This operation provides a way to update an existing invite. Updates are performed in-place; they do not change the invite link, the expiry time, nor do they re-notify the recipient of the invite.

        Reference: https://xata.io/docs/api-reference/workspaces/workspace_id/invites/invite_id#updates-an-existing-invite
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

        :param invite_id: str Invite identifier
        :param payload: dict content
        :param workspace_id: str = None The workspace identifier. Default: workspace Id from the client.

        :returns ApiResponse
        """
        if workspace_id is None:
            workspace_id = self.client.get_workspace_id()
        url_path = f"/workspaces/{workspace_id}/invites/{invite_id}"
        headers = {"content-type": "application/json"}
        return self.request("PATCH", url_path, headers, payload)

    def accept(self, invite_key: str, workspace_id: str = None) -> ApiResponse:
        """
        Accept the invitation to join a workspace. If the operation succeeds the user will be a member of the workspace

        Reference: https://xata.io/docs/api-reference/workspaces/workspace_id/invites/invite_key/accept#accept-the-invitation-to-join-a-workspace
        Path: /workspaces/{workspace_id}/invites/{invite_key}/accept
        Method: POST
        Response status codes:
        - 204: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 403: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error

        :param invite_key: str Invite Key (secret) for the invited user
        :param workspace_id: str = None The workspace identifier. Default: workspace Id from the client.

        :returns ApiResponse
        """
        if workspace_id is None:
            workspace_id = self.client.get_workspace_id()
        url_path = f"/workspaces/{workspace_id}/invites/{invite_key}/accept"
        return self.request("POST", url_path)

    def resend(self, invite_id: str, workspace_id: str = None) -> ApiResponse:
        """
        This operation provides a way to resend an Invite notification. Invite notifications can only be sent for Invites not yet accepted.

        Reference: https://xata.io/docs/api-reference/workspaces/workspace_id/invites/invite_id/resend#resend-invite-notification
        Path: /workspaces/{workspace_id}/invites/{invite_id}/resend
        Method: POST
        Response status codes:
        - 204: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 403: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error

        :param invite_id: str Invite identifier
        :param workspace_id: str = None The workspace identifier. Default: workspace Id from the client.

        :returns ApiResponse
        """
        if workspace_id is None:
            workspace_id = self.client.get_workspace_id()
        url_path = f"/workspaces/{workspace_id}/invites/{invite_id}/resend"
        return self.request("POST", url_path)
