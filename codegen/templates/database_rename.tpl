
    def ${operation_id}(self, db_name: str, new_name: str, workspace_id: str = None) -> ApiResponse:
       """
       % for line in description :
       ${line}
       % endfor

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

       :param db_name: str Current database name
       :param new_name: str New database name
       :param workspace_id: str = None The workspace identifier. Default: workspace Id from the client.

       :return Response
       """
       if workspace_id is None:
           workspace_id = self.client.get_workspace_id()
       payload = {"newName": new_name}
       url_path = f"${path}"
       headers = {"content-type": "application/json"}
       return self.request("${http_method}", url_path, headers, payload)
