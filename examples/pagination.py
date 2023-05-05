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

from xata.client import XataClient

xata = XataClient()

# Page through table `nba_teams` and get 5 records per page.
# Sort by state asc, city asc and team name desc
# Filter out teams that are not in the western conference
#
# Please refer to https://xata.io/docs/api-reference/db/db_branch_name/tables/table_name/query#query-table
# for more options for options on pagination, sorting, filters, or query conditions

# Initalize controls
more = True
cursor = None

# Loop through the pages
while more:
    # Build query statement
    query = {
        "columns": ["*"],  # Return all columns
        "page": {"size": 5, "after": cursor},  # Page size  # Cursor for next page
        "filter": {"conference": {"$is": "west"}},  # Filter for conference = west
        "sort": [
            {"state": "asc"},  # sort by state asc
            {"city": "asc"},  # sort by city asc
            {"name": "desc"},  # sort by name desc
        ],
    }

    # Only the first request can have sorting defined. Every following
    # cursor request will have the sort implied by the first request
    if cursor:
        del query["sort"]
        del query["filter"]

    # Query the data
    resp = xata.data().queryTable("nba_teams", query)

    # Print teams
    for team in resp.json()["records"]:
        print("[%s] %s: %s, %s" % (team["conference"], team["state"], team["name"], team["city"]))

    # Update controls
    more = resp.json()["meta"]["page"].get("more", False)  # has another page with results
    cursor = resp.json()["meta"]["page"]["cursor"]  # save next cursor for results

# Output:
# [west] California: LA Lakers, Los Angeles
# [west] California: LA Clippers, Los Angeles
# [west] California: Sacramento Kings, Sacramento
# [west] California: Golden State Warriors, San Francisco
# [west] Colorado: Denver Nuggets, Denver
# [west] Oklahoma: OKC Thunder, Oklahoma City
# [west] Oregon: Portland Trailblazers, Portland
# [west] Texas: Dallas Mavericks, Dallas
# [west] Texas: Houston Rockets, Houston
