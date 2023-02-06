# ------------------------------------------------------- #
# Other
# other
# Specification: workspace:v1.0
# Base URL: https://{workspaceId}.{regionId}.xata.sh
# ------------------------------------------------------- #

from requests import Response

from ..namespace import Namespace


class Other(Namespace):

    base_url = "https://{workspaceId}.{regionId}.xata.sh"
    scope    = "workspace"
