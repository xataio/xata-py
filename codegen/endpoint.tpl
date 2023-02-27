
    def ${operation_id}(self, ${', '.join([f"{p['nameParam']}: {p['type']}" for p in params['list']])}) -> Response:
       """
       % for line in description :
       ${line}
       % endfor
       Path: ${path}
       Method: ${http_method}
       Responses: 
       % for rc in params['response_codes']:
       - ${rc["code"]}: ${rc["description"]}
       % endfor

       % for param in params['list']:
       :param ${param['nameParam']}: ${param['type']} ${param['description']}
       % endfor

       :return Response
       """
       % if params['smart_db_branch_name'] :
       db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
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
       % if params['has_payload'] :
       headers = {"content-type": "application/json"}
       return self.request("${http_method}", url_path, headers, payload)
       % else :
       return self.request("${http_method}", url_path)
       % endif
