# -*- coding: utf-8 -*-

from requests import Response, request

from .errors import RateLimitException


class Namespace:
    """
    Parent class for Namespaces
    """

    def __init__(self, client):
        self.client = client
        self.is_control_plane = self.get_scope() == "core"

    def get_scope(self) -> str:
        return self.scope

    def get_base_url(self) -> str:
        if self.is_control_plane:
            return self.base_url
        return self.base_url.replace(
            "{workspaceId}", self.client.get_config()["workspaceId"]
        ).replace("{regionId}", self.client.get_config()["region"])

    def request(
        self, http_method: str, url_path: str, headers: dict = {}, payload: dict = None
    ) -> Response:
        headers = {
            **headers,
            **self.client.get_headers(),
        }  # TODO use "|" when client py min version >= 3.9
        url = "%s/%s" % (self.get_base_url(), url_path.lstrip("/"))

        if payload is None:
            resp = request(http_method, url, headers=headers)
        else:
            resp = request(http_method, url, headers=headers, json=payload)

        if resp.status_code == 429:
            raise RateLimitException(f"Rate limited: {resp.json()}")

        return resp
