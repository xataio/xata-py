
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
       # TODO handle query params
       % endif
       % if params['has_payload'] :
       headers = {"content-type": "application/json"}
       return self.request("${http_method}", url_path, headers, payload)
       % else :
       return self.request("${http_method}", url_path)
       % endif
