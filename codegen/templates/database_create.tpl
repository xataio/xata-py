
    def ${operation_id}(self, db_name: str, workspace_id: str = None, region: str = None, branch_name: str = None) -> ApiResponse:
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

       :param db_name: str The Database Name
       :param workspace_id: str = None The workspace identifier. Default: workspace Id from the client.
       :param region: str = None Which region to deploy. Default: region defined in the client, if not specified: us-east-1
       :param branch_name: str = None Which branch to create. Default: branch name used from the client, if not speicifed: main

       :return Response
       """
       if workspace_id is None:
           workspace_id = self.client.get_workspace_id()
       payload = {
         "region": region if region else self.client.get_region(),
         "branchName": branch_name if branch_name else self.client.get_branch_name(),
       }
       url_path = f"${path}"
       headers = {"content-type": "application/json"}
       return self.request("${http_method}", url_path, headers, payload)
