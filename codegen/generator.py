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
# version: 1.0.0
#

import argparse
import logging

import requests
from mako.template import Template

parser = argparse.ArgumentParser()
parser.add_argument("--scope", help="OpenAPI spec scope", type=str)
args = parser.parse_args()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

WS_DIR = "codegen/ws"  # TODO use path from py
HTTP_METHODS = ["get", "put", "post", "delete", "patch"]
SPECS = {
    "core": {
        "spec_url": "https://xata.io/api/openapi?scope=core",
        "base_url": "https://api.xata.io",
    },
    "workspace": {
        "spec_url": "https://xata.io/api/openapi?scope=workspace",
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


def generate_namespace(
    namespace: dict, scope: str, spec_version: str, spec_base_url: str
):
    """
    Generate the namespaced Class for the endpoints
    """
    vars = {
        "class_name": namespace["x-displayName"].replace(" ", "_").lower().capitalize(),
        "class_description": namespace["description"].strip()
        if "description" in namespace
        else namespace["x-displayName"].strip(),
        "spec_scope": scope,
        "spec_version": spec_version,
        "spec_base_url": spec_base_url,
    }
    out = Template(filename="codegen/namespace.tpl", output_encoding="utf-8").render(
        **vars
    )
    file_name = "%s/%s.py" % (WS_DIR, namespace["name"].replace(" ", "_").lower())
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
            file_name = "%s/%s.py" % (
                WS_DIR,
                endpoints[method]["tags"][0].replace(" ", "_").lower(),
            )
            fh = open(file_name, "a+")
            fh.write(out.decode("utf-8"))
            fh.close()
            logging.info(
                "> appended endpoint %s to %s"
                % (endpoints[method]["operationId"], file_name)
            )


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


def generate_endpoint(
    path: str, method: str, endpoint: dict, parameters: list, references: dict
) -> str:
    """
    Generate a single endpoint
    """
    if "parameters" in endpoint:
        endpointParams = get_endpoint_params(
            path, endpoint, parameters + endpoint["parameters"], references
        )
    else:
        endpointParams = get_endpoint_params(path, endpoint, parameters, references)
    if "description" in endpoint:
        desc = endpoint["description"].strip()
    else:
        desc = endpoint["summary"].strip()

    vars = {
        "operation_id": endpoint["operationId"].strip(),
        "description": desc,
        "http_method": method.upper(),
        "path": path.lower(),
        "params": endpointParams,
        "request_body": get_endpoint_request_body(endpoint),
        # "responses": list(endpoint["responses"].keys()),
    }
    return Template(filename="codegen/endpoint.tpl", output_encoding="utf-8").render(
        **vars
    )


def get_endpoint_params(
    path: str, endpoint: dict, parameters: dict, references: dict
) -> list:
    skel = {
        "list": [],
        "has_path_params": 0,
        "has_query_params": 0,
        "has_payload": False,
        "has_optional_params": 0,
    }
    if len(parameters) > 0:
        for r in parameters:
            # if not in ref -> endpoint specific params
            # else if name not in r -> method specific params
            # else fail with code: 11
            p = None
            if "$ref" in r and r["$ref"] in references:
                p = references[r["$ref"]]
                p["type"] = type_replacement(references[p["schema"]["$ref"]]["type"])
            elif "name" in r:
                p = r
                p["type"] = type_replacement(r["schema"]["type"])
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

    # Remove duplicates
    tmp = {}
    for p in skel["list"]:
        if p["name"].lower() not in tmp:
            tmp[p["name"].lower()] = p
    skel["list"] = tmp.values()

    # reorder for optional params to be last
    if skel["has_optional_params"]:
        skel["list"] = [e for e in skel["list"] if e["required"]] + [
            e for e in skel["list"] if not e["required"]
        ]
    return skel


def get_endpoint_request_body(endpoint) -> dict:
    if "requestBody" not in endpoint:
        return {}
    return {
        #     "mimetype": endpoint["requestBody"]["content"].keys()[0]
    }


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


if __name__ == "__main__":
    scope = args.scope.lower().strip()
    logging.info("starting codegen for scope: %s .." % scope)
    if scope not in SPECS:
        logging.error("unknow scope: %s" % scope)
        exit(3)
    WS_DIR += f"/{scope}"

    # fetch spec
    spec = fetch_openapi_specs(SPECS[scope]["spec_url"])

    # filter out endpointless namespaces
    logging.info(
        "pruning %d namespaces to ensure endpoints exist .." % len(spec["tags"])
    )
    namespaces = spec["tags"]
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
        logging.info(
            "[%2d/%2d] %s: %s" % (it, len(spec["paths"]), path, endpoints["summary"])
        )
        generate_endpoints(path, endpoints, references)
        it += 1
    logging.info("done.")
