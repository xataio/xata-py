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


class UnauthorizedException(Exception):
    pass


class RateLimitException(Exception):
    pass


class RecordNotFoundException(Exception):
    id: str

    def __init__(self, id: str):
        self.id = id

    def __str__(self) -> str:
        return f"Record with id {self.id} not found"


class BadRequestException(Exception):
    status_code: int
    message: str

    def __init__(self, status_code, message):
        self.status_code = status_code
        self.message = message
        super().__init__(message)

    def __str__(self) -> str:
        return f"Bad request: {self.status_code} {self.message}"


class ServerErrorException(Exception):
    status_code: int
    message: str

    def __init__(self, status_code, message):
        self.status_code = status_code
        self.message = message
        super().__init__(message)

    def __str__(self) -> str:
        return f"Server error: {self.status_code} {self.message}"
