
    def ${operation_id}(self, ${', '.join([f"{p['name']}: {p['type']}" for p in params['list']])}) -> Response:
       """
       ${description}
       path: ${path}
       method: ${http_method}

       % for param in params['list']:
       :param ${param['name']}: ${param['type']} ${param['description']} [in: ${param['in']}, req: ${param['required']}]
       % endfor

       :return Response
       """
       % if params['has_path_params'] :
       url_path = f"${path}"
       % else :
       url_path = "${path}"
       % endif
       % if params['has_query_params'] :
       query_params = []
       % for param in params['list']:
       % if param['in'] == 'query':
       if ${param['name']} is not None:
         % if param['trueType'] == 'list' :
         query_params.append("${param['name']}=%s" % ",".join(${param['name']}))
         % else :
         query_params.append(f"${param['name']}={${param['name']}}")
         % endif
       % endif
       % endfor
       if query_params:
         url_path += "?" + "&".join(query_params)
       % endif
       % if params['has_payload'] :
       headers = {"content-type": "application/json"}
       return self.request("${http_method}", url_path, headers, payload)
       % else :
       return self.request("${http_method}", url_path)
       % endif
