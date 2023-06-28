#
# Licensed to Xatabase, Inc under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Xatabase, Inc licenses this file to you under the
# Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You
# may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

from requests import Response

from xata.namespace import Namespace


class Sql(Namespace):

    scope = "core"

    def query(self, query: str, workspace_id: str = None, db_name: str = None, branch_name: str = None) -> Response:
        if workspace_id is None:
            workspace_id = self.client.get_workspace_id()
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/workspaces/{workspace_id}/db/{db_branch_name}/sql"
        #url_path = f"/db/{db_branch_name}/sql"
        headers = {"content-type": "application/json"}
        payload = {"query": query}
        return self.request("POST", url_path, headers, payload)
