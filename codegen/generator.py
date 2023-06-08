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

#
# Spec docs: https://xata.io/docs/rest-api/openapi
# version: 1.1.0
#

import argparse
import datetime
import hashlib
import json
import logging
import textwrap
from typing import Any, Dict

import coloredlogs
import requests
from mako.template import Template

from xata.helpers import to_rfc339

VERSION = "1.1.0"

parser = argparse.ArgumentParser()
parser.add_argument("--scope", help="OpenAPI spec scope", type=str)
parser.add_argument(
    "--force-generate", action="store_true", help="Always generate the endpoints even if there is no spec update."
)
args = parser.parse_args()

coloredlogs.install(level="INFO")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

WS_DIR = "codegen/ws"  # TODO use path from py
SCHEMA_OUT = {}
HTTP_METHODS = ["get", "put", "post", "delete", "patch"]
SPECS = {
    "core": {
        "spec_url": "https://xata.io/docs/api/openapi?scope=core",
        "base_url": "https://api.xata.io",
    },
    "workspace": {
        "spec_url": "https://xata.io/docs/api/openapi?scope=workspace",
        "base_url": "https://{workspaceId}.{regionId}.xata.sh",
    },
}
TYPE_REPLACEMENTS = {
    "integer": "int",
    "boolean": "bool",
    "array": "list",
    "object": "dict",
    "string": "str",
}
RESERVED_WORDS = ["from"]
REF_DB_BRANCH_NAME_PARAM = "#/components/parameters/DBBranchNameParam"
REF_WORKSPACE_ID_PARAM = "#/components/parameters/WorkspaceIDParam"
REF_WORKSPACE_ID_PARAM_EXCLUSIONS = [""]

API_RENAMING = {
    "databases": {
        "getDatabaseMetadata": "get_metadata",
        "createDatabase": "create",
        "deleteDatabase": "delete",
        "updateDatabaseMetadata": "update_metadata",
        "listRegions": "get_regions",
        "getDatabaseList": "get_databases",
    },
    "users": {"getUser": "get", "updateUser": "update", "deleteUser": "delete"},
    "workspaces": {
        "getWorkspacesList": "get_workspaces",
        "createWorkspace": "create",
        "getWorkspace": "get",
        "updateWorkspace": "update",
        "deleteWorkspace": "delete",
        "getWorkspaceMembersList": "get_members",
        "updateWorkspaceMemberRole": "update_member",
        "removeWorkspaceMember": "remove_member",
    },
    "branch": {
        "getBranchList": "get_branches",
        "getBranchDetails": "get_details",
        "createBranch": "create",
        "deleteBranch": "delete",
        "getBranchMetadata": "get_metadata",
        "updateBranchMetadata": "update_metadata",
        "getBranchStats": "get_stats",
        "resolveBranch": "resolve",
    },
    "migrations": {
        "getBranchMigrationHistory": "get_history",
        "getBranchMigrationPlan": "get_plan",
        "executeBranchMigrationPlan": "execute_plan",
        "getBranchSchemaHistory": "get_schema_history",
        "compareBranchSchemas": "compare_schemas",
        "updateBranchSchemas": "update",
        "previewBranchSchemaEdit": "preview",
        "applyBranchSchemaEdit": "apply",
        "pushBranchMigrations": "push",
    },
    "records": {
        "insertRecord": "insert",
        "getRecord": "get",
        "insertRecordWithID": "insert_with_id",
        "upsertRecordWithID": "upsert_with_id",
        "deleteRecord": "delete",
        "updateRecordWithID": "update_with_id",
        "bulkInsertTableRecords": "bulk_insert",
        "branchTransaction": "transaction",
    },
    "search_and_filter": {
        "queryTable": "query",
        "searchTable": "search_table",
        "searchBranch": "search_branch",
        "vectorSearchTable": "vector_search",
        "askTable": "ask",
        "summarizeTable": "summarize",
        "aggregateTable": "aggregate",
    },
    "table": {
        "createTable": "create",
        "deleteTable": "delete",
        "updateTable": "update",
        "getTableSchema": "get_schema",
        "setTableSchema": "set_schema",
        "getTableColumns": "get_columns",
        "addTableColumn": "add_column",
        "deleteColumn": "delete_column",
        "addColumn": "add_column",
        "getColumn": "get_column",
        "updateColumn": "update_column",
    },
    "files": {
        "getFileItem": "get_item",
        "putFileItem": "put_item",
        "deleteFileItem": "delete_item",
        "getFile": "get",
        "putFile": "put",
        "deleteFile": "delete",
    },
}


def fetch_openapi_specs(spec_url: str) -> dict:
    """
    Fetch the OpenAPI Specification and return a dict
    """
    r = requests.get(spec_url)
    logging.info("fetched the %s spec with status code: %d" % (spec_url, r.status_code))
    if r.status_code != 200:
        logging.error("could not fetch spec at: %s" % spec_url)
        exit(10)
    return r.json()


def generate_namespace(namespace: dict, scope: str, spec_version: str, spec_base_url: str):
    """
    Generate the namespaced Class for the endpoints
    """
    if "description" in namespace:
        class_desc = namespace["description"]
    else:
        class_desc = namespace["x-displayName"]
        logging.warn("missing description: %s.%s" % (scope, namespace["x-displayName"]))
    vars = {
        "class_name": namespace["x-displayName"].replace(" ", "_").lower().capitalize(),
        "class_description": class_desc.strip(),
        "spec_scope": scope,
        "spec_version": spec_version,
    }
    out = Template(filename="codegen/namespace.tpl", output_encoding="utf-8").render(**vars)
    file_name = "%s/%s.py" % (WS_DIR, _sanitize_filename(namespace["name"]))
    fh = open(file_name, "w+")
    fh.write(out.decode("utf-8"))
    fh.close()
    logging.info("created namespace class %s in %s" % (namespace["name"], file_name))


def generate_endpoints(path: str, endpoints: dict, references: dict):
    """
    Generate the endpoints of a namespace
    """
    params = endpoints["parameters"] if "parameters" in endpoints else []
    for method in HTTP_METHODS:
        if method in endpoints:
            out = generate_endpoint(path, method, endpoints[method], params, references)
            file_name = "%s/%s.py" % (WS_DIR, _sanitize_filename(endpoints[method]["tags"][0]))
            fh = open(file_name, "a+")
            fh.write(out.decode("utf-8"))
            fh.close()
            logging.info("appended endpoint %s to %s" % (endpoints[method]["operationId"], file_name))


def prune_empty_namespaces(spec: dict) -> list[str]:
    n_spaces = {}
    for n in spec["tags"]:
        n_spaces[n["name"]] = 0
    for p in spec["paths"].values():
        for method in HTTP_METHODS:
            if method in p:
                n_spaces[p[method]["tags"][0]] += 1
    namespaces = []
    for n in spec["tags"]:
        if n_spaces[n["name"]] > 0:
            namespaces.append(n)
    return namespaces


def generate_endpoint(path: str, method: str, endpoint: dict, parameters: list, references: dict) -> str:
    """
    Generate a single endpoint
    """
    if "parameters" in endpoint:
        endpointParams = get_endpoint_params(path, endpoint, parameters + endpoint["parameters"], references)
    else:
        endpointParams = get_endpoint_params(path, endpoint, parameters, references)
    if "description" in endpoint:
        desc = endpoint["description"].strip()
    else:
        logging.info("missing description for %s.%s - using summary." % (path, endpoint["operationId"]))
        desc = endpoint["summary"].strip()

    # replacements
    namespace = _sanitize_filename(endpoint["tags"][0])
    operation_id = endpoint["operationId"].strip()
    if namespace in API_RENAMING and operation_id in API_RENAMING[namespace]:
        operation_id = API_RENAMING[namespace][operation_id]
        logging.debug(
            "replacing operation id of %s.%s to %s." % (namespace, endpoint["operationId"].strip(), operation_id)
        )

    vars = {
        "operation_id": operation_id,
        "description": textwrap.wrap(desc, width=90, expand_tabs=True, fix_sentence_endings=True),
        "http_method": method.upper(),
        "path": path,
        "params": endpointParams,
    }

    SCHEMA_OUT["endpoints"].append(
        {
            "namespace": endpoint["tags"][0],
            "name": endpoint["summary"].strip(),
            "operation_id": endpoint["operationId"],
            "name_python": operation_id,
            "description": desc,
            "method": vars["http_method"],
            "url_path": path,
            "responses": endpointParams["response_codes"],
            "parameters": [
                {"name": p["name"], "description": p["description"], "in": p["in"], "required": p["required"]}
                for p in list(endpointParams["list"])
            ],
        }
    )
    return Template(filename="codegen/endpoint.tpl", output_encoding="utf-8").render(**vars)


def get_endpoint_params(path: str, endpoint: dict, parameters: dict, references: dict) -> list:
    skel = {
        "list": [],
        "has_path_params": 0,
        "has_query_params": 0,
        "has_payload": False,
        "has_optional_params": 0,
        "smart_db_branch_name": False,
        "smart_workspace_id": False,
        "response_codes": [],
        "response_content_types": [],
    }
    if len(parameters) > 0:
        # Check for convience param swaps
        curatedParamList = []
        for r in parameters:
            if "$ref" in r and r["$ref"] == REF_DB_BRANCH_NAME_PARAM:
                logging.debug("adding smart value for %s" % "#/components/parameters/DBBranchNameParam")
                # push two new params to cover for string creation
                curatedParamList.append(
                    {
                        "name": "db_name",
                        "in": "path",
                        "schema": {"type": "string"},
                        "type": "str",
                        "description": "The name of the database to query. Default: database name from the client.",
                        "required": False,
                    }
                )
                curatedParamList.append(
                    {
                        "name": "branch_name",
                        "in": "path",
                        "schema": {"type": "string"},
                        "type": "str",
                        "description": "The name of the branch to query. Default: branch name from the client.",
                        "required": False,
                    }
                )
                skel["smart_db_branch_name"] = True
            elif "$ref" in r and r["$ref"] == REF_WORKSPACE_ID_PARAM:
                # and endpoint['operationId'] not in REF_WORKSPACE_ID_PARAM_EXCLUSIONS:
                logging.debug("adding smart value for %s" % "#/components/parameters/WorkspaceIdParam")
                curatedParamList.append(
                    {
                        "name": "workspace_id",
                        "in": "path",
                        "schema": {"type": "string"},
                        "type": "str",
                        "description": "The workspace identifier. Default: workspace Id from the client.",
                        "required": False,
                    }
                )
                skel["smart_workspace_id"] = True
            else:
                curatedParamList.append(r)

        for r in curatedParamList:
            p = None
            # if not in ref: endpoint specific params
            if "$ref" in r and r["$ref"] in references:
                p = references[r["$ref"]]
                p["type"] = type_replacement(references[p["schema"]["$ref"]]["type"])
            # else if name not in r: method specific params
            elif "name" in r:
                p = r
                p["type"] = type_replacement(r["schema"]["type"])
            # else fail with code: 11
            else:
                logging.error("could resolve reference %s in the lookup." % r["$ref"])
                exit(11)

            if "required" not in p:
                p["required"] = False
            if "description" not in p:
                p["description"] = ""

            p["name"] = p["name"].strip()
            p["nameParam"] = replace_reserved_words(p["name"])
            p["description"] = p["description"].strip()
            p["trueType"] = p["type"]
            if not p["required"]:
                p["type"] += " = None"

            skel["list"].append(p)

            if p["in"] == "path":
                skel["has_path_params"] += 1
            if p["in"] == "query":
                skel["has_query_params"] += 1
            if not p["required"]:
                skel["has_optional_params"] += 1

    if "requestBody" in endpoint:
        skel["list"].append(
            {
                "name": "payload",
                "nameParam": "payload",
                "type": "dict",
                "description": "content",
                "in": "requestBody",
                "required": True,  # TODO get required
            }
        )
        skel["has_payload"] = True

    # collect response schema
    if "responses" in endpoint:
        for code in endpoint["responses"]:
            desc = ""
            if "description" in endpoint["responses"][code]:
                desc = endpoint["responses"][code]["description"].strip()
            elif "$ref" in endpoint["responses"][code] and endpoint["responses"][code]["$ref"] in references:
                desc = references[endpoint["responses"][code]["$ref"]]["description"].strip()
            skel["response_codes"].append(
                {
                    "code": code,
                    "description": desc,
                }
            )
            # get content types
            if "content" in endpoint["responses"][code]:
                int_code = int(code)
                if int_code >= 200 and int_code <= 299:
                    for ct in endpoint["responses"][code]["content"]:
                        skel["response_content_types"].append({"content_type": ct, "code": code})
    # Multiple Response Content types require option for users
    if len(skel["response_content_types"]) > 1:
        skel["has_optional_params"] = True
        ct = skel["response_content_types"][0]["content_type"].lower().strip()
        skel["list"].append(
            {
                "name": "response_content_type",
                "nameParam": "response_content_type",
                "type": 'str = "%s"' % ct,
                "trueType": "str",
                "description": "Content type of the response. Default: %s" % ct,
                "in": "responseBody",
                "required": False,
            }
        )

    # Remove duplicates
    tmp = {}
    for p in skel["list"]:
        if p["name"].lower() not in tmp:
            tmp[p["name"].lower()] = p
    skel["list"] = tmp.values()

    # reorder for optional params to be last
    if skel["has_optional_params"]:
        skel["list"] = [e for e in skel["list"] if e["required"]] + [e for e in skel["list"] if not e["required"]]
    return skel


def resolve_references(spec: dict) -> dict:
    """
    Create resolvable map of all references and apply some type conversions
    """
    references = {}
    for k, group in spec["components"].items():
        for name, component in group.items():
            if "type" in component:
                component["type"] = type_replacement(component["type"])
            references[f"#/components/{k}/{name}"] = component
    return references


def type_replacement(t: str) -> str:
    origType = t.lower()
    for isType, replacement in TYPE_REPLACEMENTS.items():
        if origType == isType:
            return replacement
    return origType


def replace_reserved_words(n: str) -> str:
    if n.lower() in RESERVED_WORDS:
        return f"_{n}"
    return n


def checksum(dictionary: Dict[str, Any]) -> str:
    """
    MD5 hash of a dictionary.
    credit: https://www.doc.ic.ac.uk/~nuric/coding/how-to-hash-a-dictionary-in-python.html
    """
    dhash = hashlib.md5()
    # We need to sort arguments so {'a': 1, 'b': 2} is
    # the same as {'b': 2, 'a': 1}
    encoded = json.dumps(dictionary, sort_keys=True).encode()
    dhash.update(encoded)
    return dhash.hexdigest()


def _sanitize_filename(n: str) -> str:
    return n.replace(" ", "_").lower()


# ------------------------------------------------------- #
#                         MAIN                            #
# ------------------------------------------------------- #
if __name__ == "__main__":
    scope = args.scope.lower().strip()
    logging.info("starting codegen for scope: %s .." % scope)
    if scope not in SPECS:
        logging.error("unknow scope: %s" % scope)
        exit(3)
    WS_DIR += f"/{scope}"

    # fetch spec
    spec = fetch_openapi_specs(SPECS[scope]["spec_url"])

    # check for changes
    this_csum = checksum(spec)
    with open(f"codegen/checksums/{scope}.txt", "r") as file:
        last_csum = file.read().rstrip()
    if this_csum == last_csum:
        if not args.force_generate:
            logging.info("no specification changes detected, nothing new to generate. stopping here.")
            action = input("> force generate (hit Enter) or stop (any key) -> ")
            if action != "":
                exit(0)
        else:
            logging.info("no spec update available, but force generate flag active.")

    # Init schema out
    SCHEMA_OUT = {
        "scope": scope,
        "version_spec": spec["info"]["version"],
        "version_codegen": VERSION,
        "checksum": this_csum,
        "generated_on": to_rfc339(datetime.datetime.now(datetime.timezone.utc)),
        "base_url": SPECS[scope]["base_url"],
        "endpoints": [],
    }

    # filter out endpointless namespaces
    logging.info("pruning %d namespaces to ensure endpoints exist .." % len(spec["tags"]))
    # namespaces = spec["tags"]
    namespaces = prune_empty_namespaces(spec)

    # resolve references
    logging.info("resolving references ..")
    references = resolve_references(spec)

    # generate namespaces
    logging.info("generating %d namespaces .." % len(namespaces))
    it = 1
    for n in namespaces:
        logging.info("[%2d/%2d] creating %s" % (it, len(namespaces), n["name"]))
        generate_namespace(n, scope, spec["info"]["version"], SPECS[scope]["base_url"])
        it += 1

    # generate paths
    logging.info("generating %d paths .." % len(spec["paths"]))
    it = 1
    for path, endpoints in spec["paths"].items():
        logging.info("[%2d/%2d] %s: %s" % (it, len(spec["paths"]), path, endpoints["summary"]))
        generate_endpoints(path, endpoints, references)
        it += 1

    # fan out schema to docs
    schema_dump = open(f"codegen/ws/{scope}.json", "w")
    json.dump(SCHEMA_OUT, schema_dump, indent=2)
    schema_dump.close()
    logging.info("persisted new schema docs.")

    # Store new checksum
    logging.info("persisting spec checksum '%s'" % this_csum)
    with open(f"codegen/checksums/{scope}.txt", "w") as file:
        file.write(this_csum)

    logging.info("done.")
