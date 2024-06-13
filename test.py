from xata.client import XataClient

xata = XataClient(db_name="xata-py", workspace_id="xata-uq2d57")
record = xata.records().get("AttachmentsIssue", "rec_cpleilidajcq52kshbpg", columns=["one_file.id"])
print(record)
