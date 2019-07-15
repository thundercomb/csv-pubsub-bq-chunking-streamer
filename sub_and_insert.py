import time
import datetime
import csv

from google.cloud import pubsub_v1
from google.cloud import bigquery

project_id = "my_project"
subscription_name = "csv_test_chunk"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_name)

bq_client = bigquery.Client()
dataset_id = 'animals'
table_id = 'pets'
table_ref = bq_client.dataset(dataset_id).table(table_id)
table = bq_client.get_table(table_ref)

def callback(message):
    if message.data:
        decoded_message = message.data.decode('utf-8')
        lines = decoded_message.splitlines()
        rows_to_insert = []

        for line in lines:
            reader = csv.reader([line])
            for row in reader:
                d = datetime.datetime.strptime(row[12],'%m/%d/%Y %H:%M:%S %p')
                row[12] = d.strftime('%Y-%m-%d %H:%M:%S')
                # Ensure that empty strings don't get a type error for numeric types
                if row[17] == '':
                    row[17] = None
                if row[19] == '':
                    row[19] = None
                if row[20] == '':
                    row[20] = None

                tuple_row = tuple(row)
                rows_to_insert.append(tuple_row)

        errors = bq_client.insert_rows(table, rows_to_insert)
        if errors != []:
            print(errors)
    message.ack()
    assert errors == []

subscriber.subscribe(subscription_path, callback=callback)

# The subscriber is non-blocking, so we must keep the main thread from
# exiting to allow it to process messages in the background.
print('Listening for messages on {}'.format(subscription_path))
while True:
    time.sleep(10)
