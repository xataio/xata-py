    def ${operation_id}(self, table_name: str, question: str, rules: list[str] = [], options: dict = {}, streaming_results: bool = False, db_name: str = None, branch_name: str = None) -> ApiResponse:
        """
        ${description}

        Reference: ${docs_url}
        Path: ${path}
        Method: ${http_method}
        Response status codes:
        - 200: Response to the question
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 429: Rate limit exceeded
        - 503: ServiceUnavailable
        - 5XX: Unexpected Error
        Responses:
        - application/json
        - text/event-stream

        :param table_name: str The Table name
        :param question: str follow up question to ask
        :param rules: list[str] specific rules you want to apply, default: []
        :param options: dict more options to adjust the query, default: {}
        :param streaming_results: bool get the results streamed, default: False
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :returns ApiResponse
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/ask"
        payload = {
          "question": question,
        }
        headers = {
            "content-type": "application/json",
            "accept": "text/event-stream" if streaming_results else "application/json",
        }
        return self.request("POST", url_path, headers, payload, is_streaming=streaming_results)
