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

# ------------------------------------------------------- #
# Branch
# Branch management.
# Specification: workspace:v1.0
# Base URL: https://{workspaceId}.{regionId}.xata.sh
# ------------------------------------------------------- #

from requests import Response

from xata.namespace import Namespace


class Branch(Namespace):

    base_url = "https://{workspaceId}.{regionId}.xata.sh"
    scope = "workspace"

    def getBranchList(self, db_name: str) -> Response:
        """
        List all available Branches
        path: /dbs/{db_name}
        method: GET

        :param db_name: str The Database Name
        :return Response
        """
        url_path = f"/dbs/{db_name}"
        return self.request("GET", url_path)

    def getBranchDetails(self, db_branch_name: str) -> Response:
        """
        Get branch schema and metadata
        path: /db/{db_branch_name}
        method: GET

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`.
        :return Response
        """
        url_path = f"/db/{db_branch_name}"
        return self.request("GET", url_path)

    def createBranch(self, db_branch_name: str, payload: dict) -> Response:
        """
        Create Database branch
        path: /db/{db_branch_name}
        method: PUT

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`.
        :param payload: dict Request Body
        :return Response
        """
        url_path = f"/db/{db_branch_name}"
        headers = {"content-type": "application/json"}
        return self.request("PUT", url_path, headers, payload)

    def deleteBranch(self, db_branch_name: str) -> Response:
        """
        Delete the branch in the database and all its resources
        path: /db/{db_branch_name}
        method: DELETE

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`.
        :return Response
        """
        url_path = f"/db/{db_branch_name}"
        return self.request("DELETE", url_path)

    def getBranchMetadata(self, db_branch_name: str) -> Response:
        """
        Get Branch Metadata
        path: /db/{db_branch_name}/metadata
        method: GET

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`.
        :return Response
        """
        url_path = f"/db/{db_branch_name}/metadata"
        return self.request("GET", url_path)

    def updateBranchMetadata(self, db_branch_name: str, payload: dict) -> Response:
        """
        Update the branch metadata
        path: /db/{db_branch_name}/metadata
        method: PUT

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`.
        :param payload: dict Request Body
        :return Response
        """
        url_path = f"/db/{db_branch_name}/metadata"
        headers = {"content-type": "application/json"}
        return self.request("PUT", url_path, headers, payload)

    def getBranchStats(self, db_branch_name: str) -> Response:
        """
        Get branch usage metrics.
        path: /db/{db_branch_name}/stats
        method: GET

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`.
        :return Response
        """
        url_path = f"/db/{db_branch_name}/stats"
        return self.request("GET", url_path)

    def getGitBranchesMapping(self, db_name: str) -> Response:
        """
               Lists all the git branches in the mapping, and their associated Xata branches.

        Example response:

        ```json
        {
          "mappings": [
              {
                "gitBranch": "main",
                "xataBranch": "main"
              },
              {
                "gitBranch": "gitBranch1",
                "xataBranch": "xataBranch1"
              }
              {
                "gitBranch": "xataBranch2",
                "xataBranch": "xataBranch2"
              }
          ]
        }
        ```
               path: /dbs/{db_name}/gitbranches
               method: GET

               :param db_name: str The Database Name
               :return Response
        """
        url_path = f"/dbs/{db_name}/gitbranches"
        return self.request("GET", url_path)

    def addGitBranchesEntry(self, db_name: str, payload: dict) -> Response:
        """
               Adds an entry to the mapping of git branches to Xata branches. The git branch and the Xata branch must be present in the body of the request. If the Xata branch doesn't exist, a 400 error is returned.

        If the git branch is already present in the mapping, the old entry is overwritten, and a warning message is included in the response. If the git branch is added and didn't exist before, the response code is 204. If the git branch existed and it was overwritten, the response code is 201.

        Example request:

        ```json
        // POST https://tutorial-ng7s8c.xata.sh/dbs/demo/gitBranches
        {
          "gitBranch": "fix/bug123",
          "xataBranch": "fix_bug"
        }
        ```
               path: /dbs/{db_name}/gitbranches
               method: POST

               :param db_name: str The Database Name
               :param payload: dict Request Body
               :return Response
        """
        url_path = f"/dbs/{db_name}/gitbranches"
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, headers, payload)

    def removeGitBranchesEntry(self, db_name: str) -> Response:
        """
               Removes an entry from the mapping of git branches to Xata branches. The name of the git branch must be passed as a query parameter. If the git branch is not found, the endpoint returns a 404 status code.

        Example request:

        ```json
        // DELETE https://tutorial-ng7s8c.xata.sh/dbs/demo/gitBranches?gitBranch=fix%2Fbug123
        ```
               path: /dbs/{db_name}/gitbranches
               method: DELETE

               :param db_name: str The Database Name
               :return Response
        """
        url_path = f"/dbs/{db_name}/gitbranches"
        return self.request("DELETE", url_path)

    def resolveBranch(self, db_name: str) -> Response:
        """
               In order to resolve the database branch, the following algorithm is used:
        * if the `gitBranch` was provided and is found in the [git branches mapping](/api-reference/dbs/db_name/gitBranches), the associated Xata branch is returned
        * else, if a Xata branch with the exact same name as `gitBranch` exists, return it
        * else, if `fallbackBranch` is provided and a branch with that name exists, return it
        * else, return the default branch of the DB (`main` or the first branch)

        Example call:

        ```json
        // GET https://tutorial-ng7s8c.xata.sh/dbs/demo/dbs/demo/resolveBranch?gitBranch=test&fallbackBranch=tsg
        ```

        Example response:

        ```json
        {
          "branch": "main",
          "reason": {
            "code": "DEFAULT_BRANCH",
            "message": "Default branch for this database (main)"
          }
        }
        ```
               path: /dbs/{db_name}/resolvebranch
               method: GET

               :param db_name: str The Database Name
               :return Response
        """
        url_path = f"/dbs/{db_name}/resolvebranch"
        return self.request("GET", url_path)
