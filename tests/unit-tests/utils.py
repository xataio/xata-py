
import re

PATTERNS_UUID4 = re.compile(r"^[\da-f]{8}-([\da-f]{4}-){3}[\da-f]{12}$", re.IGNORECASE)
PATTERNS_SDK_VERSION = re.compile(r"^[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}$")