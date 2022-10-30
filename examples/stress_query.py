import threading
import time

from xata import XataClient
from xata.client import RateLimitException

client = XataClient()
# client = XataClient(base_url_domain="staging.xatabase.co")


def run_query_loop(threadId):
    for i in range(100):
        print(f"Thread {threadId} query {i}")
        try:
            res = client.query(
                "titles",
                dbName="imdb",
                branchName="main",
                filter={"primaryTitle": {"$contains": "testingtest"}},
            )

            if res.status_code != 200:
                raise Exception("Failed to query")
            print(res.json())
        except RateLimitException as e:
            print(f"Rate limited {threadId}:", e)
            time.sleep(1)


threads = []
for i in range(50):
    t = threading.Thread(target=run_query_loop, args=(i,))
    t.start()
    threads.append(t)
for x in threads:
    x.join()
