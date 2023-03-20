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

from datetime import datetime, timezone

def to_rfc339(dt: datetime, tz = timezone.utc) -> str:
    """
    Format a datetime object to an RFC3339 compliant string
    :link https://xata.io/docs/concepts/data-model#datetime
    :link https://datatracker.ietf.org/doc/html/rfc3339

    :param dt: datetime instance to convert
    :param tz: timezone to convert in, default: UTC
    :return str
    """
    return dt.replace(tzinfo=tz).isoformat()
