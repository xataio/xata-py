# ------------------------------------------------------- #
# Users
# Users management
# Specification: core:v1.0
# Base URL: https://api.xata.io
# ------------------------------------------------------- #

from requests import Response

from xata.namespace import Namespace


class Users(Namespace):

    base_url = "https://api.xata.io"
    scope = "core"

    def getUser(
        self,
    ) -> Response:
        """
        Return details of the user making the request
        path: /user
        method: GET

        :return Response
        """
        url_path = "/user"
        return self.request("GET", url_path)

    def updateUser(self, payload: dict) -> Response:
        """
        Update user info
        path: /user
        method: PUT

        :param payload: dict Request Body
        :return Response
        """
        url_path = "/user"
        headers = {"content-type": "application/json"}
        return self.request("PUT", url_path, payload, headers)

    def deleteUser(
        self,
    ) -> Response:
        """
        Delete the user making the request
        path: /user
        method: DELETE

        :return Response
        """
        url_path = "/user"
        return self.request("DELETE", url_path)
