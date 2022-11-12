import random

from xata import XataClient

client = XataClient(
    db_url="https://xata-uq2d57.eu-west-1.xata.sh/db/nextjsconf22", branch_name="main"
)


more = True
cursor = None
emails = []
while more:
    resp = client.query("emails", page=dict(size=200, after=cursor))
    more = resp["meta"]["page"].get("more", False)
    cursor = resp["meta"]["page"]["cursor"]
    emails.extend(rec["email"] for rec in resp["records"])

winners = random.sample(emails, 50)

for winner in winners:
    print(winner)
