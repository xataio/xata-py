# ------------------------------------------------------- #
# Authentication
# Authentication and API Key management.
# Specification: core:v1.0
# Base URL: https://api.xata.io
# ------------------------------------------------------- #

from requests import Response

from xata.namespace import Namespace


class Authentication(Namespace):

    base_url = "https://api.xata.io"
    scope = "core"

    def getUserAPIKeys(
        self,
    ) -> Response:
        """
        Retrieve a list of existing user API keys
        path: /user/keys
        method: GET

        :return Response
        """
        url_path = "/user/keys"
        return self.request("GET", url_path)

    def createUserAPIKey(self, key_name: str) -> Response:
        """
        Create and return new API key
        path: /user/keys/{key_name}
        method: POST

        :param key_name: str API Key name
        :return Response
        """
        url_path = f"/user/keys/{key_name}"
        return self.request("POST", url_path)

    def deleteUserAPIKey(self, key_name: str) -> Response:
        """
        Delete an existing API key
        path: /user/keys/{key_name}
        method: DELETE

        :param key_name: str API Key name
        :return Response
        """
        url_path = f"/user/keys/{key_name}"
        return self.request("DELETE", url_path)
