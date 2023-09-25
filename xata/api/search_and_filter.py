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
# SearchAndFilter
# APIs for searching, querying, filtering, and aggregating records.
# Specification: workspace:v1.0
# ------------------------------------------------------- #

from xata.api_request import ApiRequest
from xata.api_response import ApiResponse


class SearchAndFilter(ApiRequest):

    scope = "workspace"

    def query(self, table_name: str, payload: dict = None, db_name: str = None, branch_name: str = None) -> ApiResponse:
        """
        The Query Table API can be used to retrieve all records in a table.
        The API support filtering, sorting, selecting a subset of columns, and pagination.

        The overall structure of the request looks like this:

        ```json
        // POST /db/<dbname>:<branch>/tables/<table>/query
        {
          "columns": [...],
          "filter": {
            "$all": [...],
            "$any": [...]
            ...
          },
          "sort": {
            "multiple": [...]
            ...
          },
          "page": {
            ...
          }
        }
        ```

        For usage, see also the [API Guide](https://xata.io/docs/api-guide/get).

        ### Column selection

        If the `columns` array is not specified, all columns are included. For link
        fields, only the ID column of the linked records is included in the response.

        If the `columns` array is specified, only the selected and internal
        columns `id` and `xata` are included. The `*` wildcard can be used to
        select all columns.

        For objects and link fields, if the column name of the object is specified, we
        include all of its sub-keys. If only some sub-keys are specified (via dotted
        notation, e.g. `"settings.plan"` ), then only those sub-keys from the object
        are included.

        By the way of example, assuming two tables like this:

        ```json {"truncate": true}
        {
          "tables": [
            {
              "name": "teams",
              "columns": [
                {
                  "name": "name",
                  "type": "string"
                },
                {
                  "name": "owner",
                  "type": "link",
                  "link": {
                    "table": "users"
                  }
                },
                {
                  "name": "foundedDate",
                  "type": "datetime"
                },
              ]
            },
            {
              "name": "users",
              "columns": [
                {
                  "name": "email",
                  "type": "email"
                },
                {
                  "name": "full_name",
                  "type": "string"
                },
                {
                  "name": "address",
                  "type": "object",
                  "columns": [
                    {
                      "name": "street",
                      "type": "string"
                    },
                    {
                      "name": "number",
                      "type": "int"
                    },
                    {
                      "name": "zipcode",
                      "type": "int"
                    }
                  ]
                },
                {
                  "name": "team",
                  "type": "link",
                  "link": {
                    "table": "teams"
                  }
                }
              ]
            }
          ]
        }
        ```

        A query like this:

        ```json
        POST /db/<dbname>:<branch>/tables/<table>/query
        {
          "columns": [
            "name",
            "address.*"
          ]
        }
        ```

        returns objects like:

        ```json
        {
          "name": "Kilian",
          "address": {
            "street": "New street",
            "number": 41,
            "zipcode": 10407
          }
        }
        ```

        while a query like this:

        ```json
        POST /db/<dbname>:<branch>/tables/<table>/query
        {
          "columns": [
            "name",
            "address.street"
          ]
        }
        ```

        returns objects like:

        ```json
        {
          "id": "id1"
          "xata": {
            "version": 0
          }
          "name": "Kilian",
          "address": {
            "street": "New street"
          }
        }
        ```

        If you want to return all columns from the main table and selected columns from the linked table, you can do it like this:

        ```json
        {
          "columns": ["*", "team.name"]
        }
        ```

        The `"*"` in the above means all columns, including columns of objects. This returns data like:

        ```json
        {
          "id": "id1"
          "xata": {
            "version": 0
          }
          "name": "Kilian",
          "email": "kilian@gmail.com",
          "address": {
            "street": "New street",
            "number": 41,
            "zipcode": 10407
          },
          "team": {
            "id": "XX",
            "xata": {
              "version": 0
            },
            "name": "first team"
          }
        }
        ```

        If you want all columns of the linked table, you can do:

        ```json
        {
          "columns": ["*", "team.*"]
        }
        ```

        This returns, for example:

        ```json
        {
          "id": "id1"
          "xata": {
            "version": 0
          }
          "name": "Kilian",
          "email": "kilian@gmail.com",
          "address": {
            "street": "New street",
            "number": 41,
            "zipcode": 10407
          },
          "team": {
            "id": "XX",
            "xata": {
              "version": 0
            },
            "name": "first team",
            "code": "A1",
            "foundedDate": "2020-03-04T10:43:54.32Z"
          }
        }
        ```

        ### Filtering

        There are two types of operators:

        - Operators that work on a single column: `$is`, `$contains`, `$pattern`,
          `$includes`, `$gt`, etc.
        - Control operators that combine multiple conditions: `$any`, `$all`, `$not` ,
          `$none`, etc.

        All operators start with an `$` to differentiate them from column names
        (which are not allowed to start with a dollar sign).

        #### Exact matching and control operators

        Filter by one column:

        ```json
        {
          "filter": {
            "<column_name>": "value"
          }
        }
        ```

        This is equivalent to using the `$is` operator:

        ```json
        {
          "filter": {
            "<column_name>": {
              "$is": "value"
            }
          }
        }
        ```

        For example:

        ```json
        {
          "filter": {
            "name": "r2"
          }
        }
        ```

        Or:

        ```json
        {
          "filter": {
            "name": {
              "$is": "r2"
            }
          }
        }
        ```

        For objects, both dots and nested versions work:

        ```json
        {
          "filter": {
            "settings.plan": "free"
          }
        }
        ```

        ```json
        {
          "filter": {
            "settings": {
              "plan": "free"
            }
          }
        }
        ```

        If you want to OR together multiple values, you can use the `$any` operator with an array of values:

        ```json
        {
          "filter": {
            "settings.plan": { "$any": ["free", "paid"] }
          }
        }
        ```

        If you specify multiple columns in the same filter, they are logically AND'ed together:

        ```json
        {
          "filter": {
            "settings.dark": true,
            "settings.plan": "free"
          }
        }
        ```

        The above matches if both conditions are met.

        To be more explicit about it, you can use `$all` or `$any`:

        ```json
        {
          "filter": {
            "$any": {
              "settings.dark": true,
              "settings.plan": "free"
            }
          }
        }
        ```

        The `$all` and `$any` operators can also receive an array of objects, which allows for repeating column names:

        ```json
        {
          "filter": {
            "$any": [
              {
                "name": "r1"
              },
              {
                "name": "r2"
              }
            ]
          }
        }
        ```

        You can check for a value being not-null with `$exists`:

        ```json
        {
          "filter": {
            "$exists": "settings"
          }
        }
        ```

        This can be combined with `$all` or `$any` :

        ```json
        {
          "filter": {
            "$all": [
              {
                "$exists": "settings"
              },
              {
                "$exists": "name"
              }
            ]
          }
        }
        ```

        Or you can use the inverse operator `$notExists`:

        ```json
        {
          "filter": {
            "$notExists": "settings"
          }
        }
        ```

        #### Partial match

        `$contains` is the simplest operator for partial matching. Note that `$contains` operator can
        cause performance issues at scale, because indices cannot be used.

        ```json
        {
          "filter": {
            "<column_name>": {
              "$contains": "value"
            }
          }
        }
        ```

        Wildcards are supported via the `$pattern` operator:

        ```json
        {
          "filter": {
            "<column_name>": {
              "$pattern": "v*alu?"
            }
          }
        }
        ```

        The `$pattern` operator accepts two wildcard characters:
        * `*` matches zero or more characters
        * `?` matches exactly one character

        If you want to match a string that contains a wildcard character, you can escape them using a backslash (`\`). You can escape a backslash by usign another backslash.

        You can also use the `$endsWith` and `$startsWith` operators:

        ```json
        {
          "filter": {
            "<column_name>": {
              "$endsWith": ".gz"
            },
            "<column_name>": {
              "$startsWith": "tmp-"
            }
          }
        }
        ```

        #### Numeric or datetime ranges

        ```json
        {
          "filter": {
            "<column_name>": {
              "$ge": 0,
              "$lt": 100
            }
          }
        }
        ```
        Date ranges support the same operators, with the date using the format defined in
        [RFC 3339](https://www.rfc-editor.org/rfc/rfc3339):
        ```json
        {
          "filter": {
            "<column_name>": {
              "$gt": "2019-10-12T07:20:50.52Z",
              "$lt": "2021-10-12T07:20:50.52Z"
            }
          }
        }
        ```
        The supported operators are `$gt`, `$lt`, `$ge`, `$le`.

        #### Negations

        A general `$not` operator can inverse any operation.

        ```json
        {
          "filter": {
            "$not": {
              "<column_name1>": "value1",
              "<column_name2>": "value1"
            }
          }
        }
        ```

        Note: in the above the two condition are AND together, so this does (NOT ( ...
        AND ...))

        Or more complex:

        ```json
        {
          "filter": {
            "$not": {
              "$any": [
                {
                  "<column_name1>": "value1"
                },
                {
                  "$all": [
                    {
                      "<column_name2>": "value2"
                    },
                    {
                      "<column_name3>": "value3"
                    }
                  ]
                }
              ]
            }
          }
        }
        ```

        The `$not: { $any: {}}` can be shorted using the `$none` operator:

        ```json
        {
          "filter": {
            "$none": {
              "<column_name1>": "value1",
              "<column_name2>": "value1"
            }
          }
        }
        ```

        In addition, you can use operators like `$isNot` or `$notExists` to simplify expressions:

        ```json
        {
          "filter": {
            "<column_name>": {
              "$isNot": "2019-10-12T07:20:50.52Z"
            }
          }
        }
        ```

        #### Working with arrays

        To test that an array contains a value, use `$includesAny`.

        ```json
        {
          "filter": {
            "<array_name>": {
              "$includesAny": "value"
            }
          }
        }
        ```

        ##### `includesAny`

        The `$includesAny` operator accepts a custom predicate that will check if
        any value in the array column matches the predicate. The `$includes` operator is a
        synonym for the `$includesAny` operator.

        For example a complex predicate can include
        the `$all` , `$contains` and `$endsWith` operators:

        ```json
        {
          "filter": {
            "<array name>": {
              "$includes": {
                "$all": [
                  { "$contains": "label" },
                  { "$not": { "$endsWith": "-debug" } }
                ]
              }
            }
          }
        }
        ```

        ##### `includesNone`

        The `$includesNone` operator succeeds if no array item matches the
        predicate.

        ```json
        {
          "filter": {
            "settings.labels": {
              "$includesNone": [{ "$contains": "label" }]
            }
          }
        }
        ```
        The above matches if none of the array values contain the string "label".

        ##### `includesAll`

        The `$includesAll` operator succeeds if all array items match the
        predicate.

        Here is an example of using the `$includesAll` operator:

        ```json
        {
          "filter": {
            "settings.labels": {
              "$includesAll": [{ "$contains": "label" }]
            }
          }
        }
        ```

        The above matches if all array values contain the string "label".

        ### Sorting

        Sorting by one element:

        ```json
        POST /db/demo:main/tables/table/query
        {
          "sort": {
            "index": "asc"
          }
        }
        ```

        or descendently:

        ```json
        POST /db/demo:main/tables/table/query
        {
          "sort": {
            "index": "desc"
          }
        }
        ```

        Sorting by multiple fields:

        ```json
        POST /db/demo:main/tables/table/query
        {
          "sort": [
            {
              "index": "desc"
            },
            {
              "createdAt": "desc"
            }
          ]
        }
        ```

        It is also possible to sort results randomly:

        ```json
        POST /db/demo:main/tables/table/query
        {
          "sort": {
            "*": "random"
          }
        }
        ```

        Note that a random sort does not apply to a specific column, hence the special column name `"*"`.

        A random sort can be combined with an ascending or descending sort on a specific column:

        ```json
        POST /db/demo:main/tables/table/query
        {
          "sort": [
            {
              "name": "desc"
            },
            {
              "*": "random"
            }
          ]
        }
        ```

        This will sort on the `name` column, breaking ties randomly.

        ### Pagination

        We offer cursor pagination and offset pagination. The cursor pagination method can be used for sequential scrolling with unrestricted depth. The offset pagination can be used to skip pages and is limited to 1000 records.

        Example of cursor pagination:

        ```json
        POST /db/demo:main/tables/table/query
        {
          "page": {
            "after":"fMoxCsIwFIDh3WP8c4amDai5hO5SJCRNfaVSeC9b6d1FD"
          }
        }
        ```

        In the above example, the value of the `page.after` parameter is the cursor returned by the previous query. A sample response is shown below:

        ```json
        {
          "meta": {
            "page": {
              "cursor": "fMoxCsIwFIDh3WP8c4amDai5hO5SJCRNfaVSeC9b6d1FD",
              "more": true
            }
          },
          "records": [...]
        }
        ```

        The `page` object might contain the follow keys, in addition to `size` and `offset` that were introduced before:

        - `after`: Return the next page 'after' the current cursor
        - `before`: Return the previous page 'before' the current cursor.
        - `start`: Resets the given cursor position to the beginning of the query result set.
        Will return the first N records from the query result, where N is the `page.size` parameter.
        - `end`: Resets the give cursor position to the end for the query result set.
        Returns the last N records from the query result, where N is the `page.size` parameter.

        The request will fail if an invalid cursor value is given to `page.before`,
        `page.after`, `page.start` , or `page.end`. No other cursor setting can be
        used if `page.start` or `page.end` is set in a query.

        If both `page.before` and `page.after` parameters are present we treat the
        request as a range query. The range query will return all entries after
        `page.after`, but before `page.before`, up to `page.size` or the maximum
        page size. This query requires both cursors to use the same filters and sort
        settings, plus we require `page.after < page.before`. The range query returns
        a new cursor. If the range encompass multiple pages the next page in the range
        can be queried by update `page.after` to the returned cursor while keeping the
        `page.before` cursor from the first range query.

        The `filter` , `columns`, `sort` , and `page.size` configuration will be
        encoded with the cursor. The pagination request will be invalid if
        `filter` or `sort` is set. The columns returned and page size can be changed
        anytime by passing the `columns` or `page.size` settings to the next query.

        In the following example of size + offset pagination we retrieve the third page of up to 100 results:

        ```json
        POST /db/demo:main/tables/table/query
        {
          "page": {
            "size": 100,
            "offset": 200
          }
        }
        ```

        The `page.size` parameter represents the maximum number of records returned by this query. It has a default value of 20 and a maximum value of 200.
        The `page.offset` parameter represents the number of matching records to skip. It has a default value of 0 and a maximum value of 800.

        Cursor pagination also works in combination with offset pagination. For example, starting from a specific cursor position, using a page size of 200 and an offset of 800, you can skip up to 5 pages of 200 records forwards or backwards from the cursor's position:

        ```json
        POST /db/demo:main/tables/table/query
        {
          "page": {
            "size": 200,
            "offset": 800,
            "after": "fMoxCsIwFIDh3WP8c4amDai5hO5SJCRNfaVSeC9b6d1FD"
          }
        }
        ```

        **Special cursors:**

        - `page.after=end`: Result points past the last entry. The list of records
          returned is empty, but `page.meta.cursor` will include a cursor that can be
          used to "tail" the table from the end waiting for new data to be inserted.
        - `page.before=end`: This cursor returns the last page.
        - `page.start=$cursor`: Start at the beginning of the result set of the $cursor query. This is equivalent to querying the
          first page without a cursor but applying `filter` and `sort` . Yet the `page.start`
          cursor can be convenient at times as user code does not need to remember the
          filter, sort, columns or page size configuration. All these information are
          read from the cursor.
        - `page.end=$cursor`: Move to the end of the result set of the $cursor query. This is equivalent to querying the
          last page with `page.before=end`, `filter`, and `sort` . Yet the
          `page.end` cursor can be more convenient at times as user code does not
          need to remember the filter, sort, columns or page size configuration. All
          these information are read from the cursor.

        When using special cursors like `page.after="end"` or `page.before="end"`, we
        still allow `filter` and `sort` to be set.

        Example of getting the last page:

        ```json
        POST /db/demo:main/tables/table/query
        {
          "page": {
            "size": 10,
            "before": "end"
          }
        }
        ```

        Reference: https://xata.io/docs/api-reference/db/db_branch_name/tables/table_name/query#query-table
        Path: /db/{db_branch_name}/tables/{table_name}/query
        Method: POST
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 503: ServiceUnavailable
        - 5XX: Unexpected Error
        - default: Unexpected Error

        :param table_name: str The Table name
        :param payload: dict content
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :returns ApiResponse
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/query"
        headers = {"content-type": "application/json"}
        if not payload:
            payload = {}
        return self.request("POST", url_path, headers, payload)

    def search_branch(self, payload: dict, db_name: str = None, branch_name: str = None) -> ApiResponse:
        """
        Run a free text search operation across the database branch.

        Reference: https://xata.io/docs/api-reference/db/db_branch_name/search#free-text-search
        Path: /db/{db_branch_name}/search
        Method: POST
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 503: ServiceUnavailable
        - 5XX: Unexpected Error
        - default: Unexpected Error

        :param payload: dict content
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :returns ApiResponse
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/search"
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, headers, payload)

    def search_table(self, table_name: str, payload: dict, db_name: str = None, branch_name: str = None) -> ApiResponse:
        """
        Run a free text search operation in a particular table.

        The endpoint accepts a `query` parameter that is used for the free text search and a set of structured filters (via the `filter` parameter) that are applied before the search. The `filter` parameter uses the same syntax as the [query endpoint](/api-reference/db/db_branch_name/tables/table_name/) with the following exceptions:
        * filters `$contains`, `$startsWith`, `$endsWith` don't work on columns of type `text`
        * filtering on columns of type `multiple` is currently unsupported

        Reference: https://xata.io/docs/api-reference/db/db_branch_name/tables/table_name/search#free-text-search-in-a-table
        Path: /db/{db_branch_name}/tables/{table_name}/search
        Method: POST
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error
        - default: Unexpected Error

        :param table_name: str The Table name
        :param payload: dict content
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :returns ApiResponse
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/search"
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, headers, payload)

    def vector_search(
        self, table_name: str, payload: dict, db_name: str = None, branch_name: str = None
    ) -> ApiResponse:
        """
        This endpoint can be used to perform vector-based similarity searches in a table.
        It can be used for implementing semantic search and product recommendation. To use this
        endpoint, you need a column of type vector. The input vector must have the same
        dimension as the vector column.

        Reference: https://xata.io/docs/api-reference/db/db_branch_name/tables/table_name/vectorSearch#vector-similarity-search-in-a-table
        Path: /db/{db_branch_name}/tables/{table_name}/vectorSearch
        Method: POST
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error
        - default: Unexpected Error

        :param table_name: str The Table name
        :param payload: dict content
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :returns ApiResponse
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/vectorSearch"
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, headers, payload)

    def ask(
        self,
        table_name: str,
        question: str,
        rules: list[str] = [],
        options: dict = {},
        streaming_results: bool = False,
        db_name: str = None,
        branch_name: str = None,
    ) -> ApiResponse:
        """
        Ask your table a question. If the `Accept` header is set to `text/event-stream`, Xata will stream the results back as SSE's.

        Reference: https://xata.io/docs/api-reference/db/db_branch_name/tables/table_name/ask#ask-your-table-a-question
        Path: /db/{db_branch_name}/tables/{table_name}/ask
        Method: POST
        Response status codes:
        - 200: Response to the question
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 429: Rate limit exceeded
        - 503: ServiceUnavailable
        - 5XX: Unexpected Error
        Responses:
        - application/json
        - text/event-stream

        :param table_name: str The Table name
        :param question: str follow up question to ask
        :param rules: list[str] specific rules you want to apply, default: []
        :param options: dict more options to adjust the query, default: {}
        :param streaming_results: bool get the results streamed, default: False
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :returns ApiResponse
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/ask"
        payload = {
            "question": question,
        }
        headers = {
            "content-type": "application/json",
            "accept": "text/event-stream" if streaming_results else "application/json",
        }
        return self.request("POST", url_path, headers, payload, is_streaming=streaming_results)

    def ask_follow_up(
        self,
        table_name: str,
        session_id: str,
        question: str,
        streaming_results: bool = False,
        db_name: str = None,
        branch_name: str = None,
    ) -> ApiResponse:
        """
        Ask a follow-up question. If the `Accept` header is set to `text/event-stream`, Xata will stream the results back as SSE's.

        Reference: https://xata.io/docs/api-reference/db/db_branch_name/tables/table_name/ask/session_id#continue-a-conversation-with-your-data
        Path: /db/{db_branch_name}/tables/{table_name}/ask/{session_id}
        Method: POST
        Response status codes:
        - 200: Response to the question
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 429: Rate limit exceeded
        - 503: ServiceUnavailable
        - 5XX: Unexpected Error
        Responses:
        - application/json
        - text/event-stream

        :param table_name: str The Table name
        :param session_id: str Session id from initial question
        :param question: str follow up question to ask
        :param streaming_results: bool get the results streamed, default: False
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :returns ApiResponse
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/ask/{session_id}"
        payload = {
            "message": question,
        }
        headers = {
            "content-type": "application/json",
            "accept": "text/event-stream" if streaming_results else "application/json",
        }
        return self.request("POST", url_path, headers, payload, is_streaming=streaming_results)

    def summarize(self, table_name: str, payload: dict, db_name: str = None, branch_name: str = None) -> ApiResponse:
        """
        This endpoint allows you to (optionally) define groups, and then to run
        calculations on the values in each group. This is most helpful when
        you'd like to understand the data you have in your database.

        A group is a combination of unique values. If you create a group for
        `sold_by`, `product_name`, we will return one row for every combination
        of `sold_by` and `product_name` you have in your database. When you
        want to calculate statistics, you define these groups and ask Xata to
        calculate data on each group.

        **Some questions you can ask of your data:**

        How many records do I have in this table?
        - Set `columns: []` as we we want data from the entire table, so we ask
        for no groups.
        - Set `summaries: {"total": {"count": "*"}}` in order to see the count
        of all records. We use `count: *` here we'd like to know the total
        amount of rows; ignoring whether they are `null` or not.

        What are the top total sales for each product in July 2022 and sold
        more than 10 units?
        - Set `filter: {soldAt: {
          "$ge": "2022-07-01T00:00:00.000Z",
          "$lt": "2022-08-01T00:00:00.000Z"}
        }`
        in order to limit the result set to sales recorded in July 2022.
        - Set `columns: [product_name]` as we'd like to run calculations on
        each unique product name in our table. Setting `columns` like this will
        produce one row per unique product name.
        - Set `summaries: {"total_sales": {"count": "product_name"}}` as we'd
        like to create a field called "total_sales" for each group. This field
        will count all rows in each group with non-null product names.
        - Set `sort: [{"total_sales": "desc"}]` in order to bring the rows with
        the highest total_sales field to the top.
        - Set `summariesFilter: {"total_sales": {"$ge": 10}}` to only send back data
        with greater than or equal to 10 units.

        `columns`: tells Xata how to create each group. If you add `product_id`
        we will create a new group for every unique `product_id`.

        `summaries`: tells Xata which calculations to run on each group. Xata
        currently supports count, min, max, sum, average.

        `sort`: tells Xata in which order you'd like to see results. You may
        sort by fields specified in `columns` as well as the summary names
        defined in `summaries`.

        note: Sorting on summarized values can be slower on very large tables;
        this will impact your rate limit significantly more than other queries.
        Try use `filter` to reduce the amount of data being processed in order
        to reduce impact on your limits.

        `summariesFilter`: tells Xata how to filter the results of a summary.
        It has the same syntax as `filter`, however, by using `summariesFilter`
        you may also filter on the results of a query.

        note: This is a much slower to use than `filter`. We recommend using
        `filter` wherever possible and `summariesFilter` when it's not
        possible to use `filter`.

        `page.size`: tells Xata how many records to return. If unspecified, Xata
        will return the default size.

        Reference: https://xata.io/docs/api-reference/db/db_branch_name/tables/table_name/summarize#summarize-table
        Path: /db/{db_branch_name}/tables/{table_name}/summarize
        Method: POST
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error
        - default: Unexpected Error

        :param table_name: str The Table name
        :param payload: dict content
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :returns ApiResponse
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/summarize"
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, headers, payload)

    def aggregate(self, table_name: str, payload: dict, db_name: str = None, branch_name: str = None) -> ApiResponse:
        """
        This endpoint allows you to run aggregations (analytics) on the data from one table.
        While the summary endpoint is served from a transactional store and the results are strongly
        consistent, the aggregate endpoint is served from our columnar store and the results are
        only eventually consistent. On the other hand, the aggregate endpoint uses a
        store that is more appropiate for analytics, makes use of approximative algorithms
        (e.g for cardinality), and is generally faster and can do more complex aggregations.

        For usage, see the [API Guide](https://xata.io/docs/api-guide/aggregate).

        Reference: https://xata.io/docs/api-reference/db/db_branch_name/tables/table_name/aggregate#run-aggregations-over-a-table
        Path: /db/{db_branch_name}/tables/{table_name}/aggregate
        Method: POST
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error
        - default: Unexpected Error

        :param table_name: str The Table name
        :param payload: dict content
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :returns ApiResponse
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/aggregate"
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, headers, payload)
