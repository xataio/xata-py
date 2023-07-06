
    def ${operation_id}(self, name: str, slug: str = None) -> ApiResponse:
       """
       % for line in description :
       ${line}
       % endfor

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

       :param name: str Workspace name
       :param slug: str = None Slug to use

       :return Response
       """
       payload = {"name": name}
       if slug:
          payload["slug"] = slug
       url_path = f"${path}"
       headers = {"content-type": "application/json"}
       return self.request("${http_method}", url_path, headers, payload)
