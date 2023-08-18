
    %if params['list']:
    def ${operation_id}(self, table_name: str, payload: dict = None, db_name: str = None, branch_name: str = None) -> ApiResponse:
    %else:
    def ${operation_id}(self) -> ApiResponse:
    %endif
       """
${description}

Reference: ${docs_url}
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
       db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
       url_path = f"/db/{db_branch_name}/tables/{table_name}/query"
       headers = {"content-type": "application/json"}
       if not payload:
         payload = {} 
       return self.request("POST", url_path, headers, payload)
