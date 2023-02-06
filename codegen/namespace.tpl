# ------------------------------------------------------- #
# ${class_name}
# ${class_description}
# Specification: ${spec_scope}:v${spec_version}
# Base URL: ${spec_base_url}
# ------------------------------------------------------- #

from requests import Response

from xata.namespace import Namespace


class ${class_name}(Namespace):

    base_url = "${spec_base_url}"
    scope    = "${spec_scope}"
