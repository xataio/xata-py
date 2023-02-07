# -*- coding: utf-8 -*-
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
        "class_description": namespace["description"]
        if "description" in namespace
        else namespace["x-displayName"],
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
    for method in HTTP_METHODS:
        if method in endpoints:
            params = endpoints["parameters"] if "parameters" in endpoints else []
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
    vars = {
        "operation_id": endpoint["operationId"],
        "description": endpoint["description"]
        if "description" in endpoint
        else endpoint["summary"],
        "http_method": method.upper(),
        "path": path.lower(),
        "params": get_endpoint_params(path, endpoint, parameters, references),
        "request_body": get_endpoint_request_body(endpoint),
        #        "responses": list(endpoint["responses"].keys()),
    }
    out = Template(filename="codegen/endpoint.tpl", output_encoding="utf-8").render(
        **vars
    )
    return out


def get_endpoint_params(
    path: str, endpoint: dict, parameters: dict, references: dict
) -> list:
    skel = {
        "list": [],
        "has_path_params": False,
        "has_payload": False,
    }
    if len(parameters) > 0:
        skel["has_path_params"] = True
        for r in parameters:
            if r["$ref"] not in references:
                logging.error("could resolve reference %s in the lookup." % r["$ref"])
                exit(11)
            skel["list"].append(
                {
                    "name": references[r["$ref"]]["name"],
                    "type": references[references[r["$ref"]]["schema"]["$ref"]]["type"],
                    "description": references[r["$ref"]]["description"],
                }
            )

    if "requestBody" in endpoint:
        skel["list"].append(
            {"name": "payload", "type": "dict", "description": "Request Body"}
        )
        skel["has_payload"] = True
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
                if component["type"].lower() == "string":
                    component["type"] = "str"
                if component["type"].lower() == "object":
                    component["type"] = "dict"
                if component["type"].lower() == "array":
                    component["type"] = "list"
            references[f"#/components/{k}/{name}"] = component
    return references


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
