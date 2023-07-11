
    %if params['list']:
    def ${operation_id}(self, ${', '.join([f"{p['nameParam']}: {p['type']}" for p in params['list']])}) -> ApiResponse:
    %else:
    def ${operation_id}(self) -> ApiResponse:
    %endif
       """
       % for line in description :
       ${line}
       % endfor

       Path: ${path}
       Method: ${http_method}
       % if status == "experimental":
       Status: Experimental
       % endif
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

       % for param in params['list']:
       :param ${param['nameParam']}: ${param['type']} ${param['description']}
       % endfor

       :returns ApiResponse
       """
       % if params['smart_db_branch_name'] :
       db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
       % endif
       % if params['smart_workspace_id'] :
       if workspace_id is None:
           workspace_id = self.client.get_workspace_id()
       % endif
       % if params['has_path_params'] :
       url_path = f"${path}"
       % else :
       url_path = "${path}"
       % endif
       % if params['has_query_params'] > 1:
       query_params = []
       % for param in params['list']:
       % if param['in'] == 'query':
       if ${param['nameParam']} is not None:
         % if param['trueType'] == 'list' :
         query_params.append("${param['name']}=%s" % ",".join(${param['nameParam']}))
         % else :
         query_params.append(f"${param['name']}={${param['nameParam']}}")
         % endif
       % endif
       % endfor
       if query_params:
         url_path += "?" + "&".join(query_params)
       % endif
       % if params['has_query_params'] == 1 :
       % for param in params['list']:
       % if param['in'] == 'query':
       if ${param['nameParam']} is not None:
         % if param['trueType'] == 'list' :
         url_path += "?${param['name']}=%s" % ",".join(${param['nameParam']})
         % else :
         url_path += "?${param['name']}={${param['nameParam']}}"
         % endif
       % endif
       % endfor
       % endif
       % if params['has_payload'] and len(params['response_content_types']) > 1:
       headers = {
           "content-type": "application/json",
           "accept": response_content_type,
       }
       return self.request("${http_method}", url_path, headers, payload)
       % elif params['has_payload']:
       headers = {"content-type": "application/json"}
       return self.request("${http_method}", url_path, headers, payload)
       % elif len(params['response_content_types']) > 1:
       headers = {"accept": response_content_type}
       return self.request("${http_method}", url_path, headers)
       % else :
       return self.request("${http_method}", url_path)
       % endif
