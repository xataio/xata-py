
    def ${operation_id}(
        self,
        statement: str,
        params: list = None,
        consistency: str = "strong",
        db_name: str = None,
        branch_name: str = None,
    ) -> ApiResponse:
       """
       ${description}

       Reference: ${docs_url}
       Path: ${path}
       Method: ${http_method}
       Response status codes:
       % for rc in params['response_codes']:
       - ${rc["code"]}: ${rc["description"]}
       % endfor
       % if len(params['response_content_types']) > 1 :
       Responses:
       % for rc in params['response_content_types']:
       - ${rc["content_type"]}
       % endfor
       % elif len(params['response_content_types']) == 1 :
       Response: ${params['response_content_types'][0]["content_type"]}
       % endif

       :param statement: str The statement to run
       :param params: dict The query parameters list. default: None
       :param consistency: str The consistency level for this request. default: strong
       :param db_name: str = None The name of the database to query. Default: database name from the client.
       :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

       :returns ApiResponse
       """
       db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
       url_path = f"/db/{db_branch_name}/sql"
       headers = {"content-type": "application/json"}
       payload = {
         "statement": statement,
         "params": params,
         "consistency": consistency,
       }
       return self.request("POST", url_path, headers, payload)
