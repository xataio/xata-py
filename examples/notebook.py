from xata import XataClient
import json

#client = XataClient(base_url_domain="staging.xatabase.co")
client = XataClient()

res = client.post("/db/heavyTable:main/tables/users/summarize", json={
    "summaries": {
        "total": {
            "count": "*"
        }
    }
})
print(json.dumps(res.json(), indent=2))