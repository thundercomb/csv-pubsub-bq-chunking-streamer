import requests as rq
from google.cloud import pubsub

project_id = "my_project"
topic_name = "csv_test_chunk"
url = "https://data.kingcounty.gov/api/views/yaai-7frk/rows.csv?accessType=DOWNLOAD"
publisher = pubsub.PublisherClient(batch_settings=pubsub.types.BatchSettings(max_latency=5))
topic_path = publisher.topic_path(project_id, topic_name)

response = rq.get(url)
lines = response.text.splitlines()

chunk_size = 50

count = 0
chunk = []
for line in lines[1:]:
    if count < chunk_size:
        chunk.append(line)
        count += 1
    if count == chunk_size:
        bytes_chunk = bytes("\r\n".join(chunk).encode('utf-8'))
        publisher.publish(topic_path, data=bytes_chunk)
        chunk = []
        count = 0
if count > 0:
    bytes_chunk = bytes("\r\n".join(chunk).encode('utf-8'))
    publisher.publish(topic_path, data=bytes_chunk)
