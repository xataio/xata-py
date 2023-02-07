# Code Generator

We use an OpenAPI specification to define Xata's server API, which is divided in two scopes:
* `core`: Control Plane ([spec](https://xata.io/api/openapi?scope=core))
* `workspace`: Data Plane ([spec](https://xata.io/api/openapi?scope=workspace))

Please refer to our [documentation](https://xata.io/docs/rest-api/openapi) for more information.

To run the generator go to the project root and execute the following make targets:
```
make code-gen code-gen-copy lint scope=workspace
```

`code-gen`: generates the endpoints based on the provide `scope`
`code-gen-copy`: copies the generated classes into the target directory
`lint`: runs linting in order to comply with coding standards
