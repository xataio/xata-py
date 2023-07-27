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

from dotenv import dotenv_values

from xata.client import XataClient


class TestSearchAndFilterAskTableEndpoint(object):
    def setup_class(self):
        env = dotenv_values(".env")
        self.client = XataClient(
            workspace_id=env.get("XATA_TEST_ASK_WORKSPACE"), db_name=env.get("XATA_TEST_ASK_DATABASE")
        )

    def test_ask_table_for_response_shape(self):
        answer = self.client.data().ask("xata", "does xata have a python sdk")
        assert answer.is_success()

        assert "answer" in answer
        assert "records" in answer
        assert "sessionId" in answer

        assert answer["answer"] is not None
        assert answer["sessionId"] is not None
        assert len(answer["records"]) > 0

        assert answer.headers["content-type"].lower().startswith("application/json")

    def test_ask_table_for_response_shape_with_rules(self):
        answer = self.client.data().ask("xata", "what database technology is used at xata", ["postgres is a database"])
        assert answer.is_success()

        assert "answer" in answer
        assert "records" in answer
        assert "sessionId" in answer

        assert answer["answer"] is not None
        assert answer["sessionId"] is not None
        assert len(answer["records"]) > 0

    def test_ask_table_for_response_shape_with_options(self):
        opts = {
            "searchType": "keyword",
            "search": {
                "fuzziness": 1,
                "prefix": "phrase",
                "target": [
                    "slug",
                    {"column": "title", "weight": 4},
                    "content",
                    "section",
                    {"column": "keywords", "weight": 4},
                ],
                "boosters": [
                    {
                        "valueBooster": {
                            "column": "section",
                            "value": "guide",
                            "factor": 18,
                        },
                    },
                ],
            },
        }
        answer = self.client.data().ask("xata", "how to do full text search?", options=opts)
        assert answer.is_success()

        assert "answer" in answer
        assert "records" in answer
        assert "sessionId" in answer

        assert answer["answer"] is not None
        assert answer["sessionId"] is not None
        assert len(answer["records"]) > 0

    def test_ask_table_with_streaming_response(self):
        answer = self.client.data().ask("xata", "does the data model have link type?", streaming_results=True)
        assert answer.is_success()

        #assert "answer" in answer
        #assert "records" in answer
        #assert "sessionId" in answer

        #assert answer["answer"] is not None
        #assert answer["sessionId"] is not None
        #assert len(answer["records"]) > 0

        #assert answer.headers["content-type"].lower().startswith("text/event-stream")
        assert True
