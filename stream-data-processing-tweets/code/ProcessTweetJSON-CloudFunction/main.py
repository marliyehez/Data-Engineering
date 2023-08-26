from google.cloud import storage, bigquery
import json

def hello_gcs(event, context):
     """Triggered by a change to a Cloud Storage bucket.
     Args:
          event (dict): Event payload.
          context (google.cloud.functions.Context): Metadata for the event.
     """
     file = event
     bucket_name = file['bucket']
     file_name = file['name']

     # Check if the uploaded file is a JSON file
     if not file_name.endswith('.json'):
          return

     # Initialize Cloud Storage and BigQuery clients
     storage_client = storage.Client()
     bigquery_client = bigquery.Client()

     # Get the bucket and file objects
     bucket = storage_client.bucket(bucket_name)
     blob = bucket.blob(file_name)

     # Download and parse JSON content
     json_content = blob.download_as_text()
     json_data = json.loads(json_content)

     # Insert JSON data into BigQuery
     dataset_id = 'tweet_dataset'
     table_id = 'tweet'
     dataset_ref = bigquery_client.dataset(dataset_id)
     table_ref = dataset_ref.table(table_id)
     table = bigquery_client.get_table(table_ref)

     cols = ['target', 'ids', 'date', 'user', 'text']
     row_to_insert = [tuple(json_data[col] for col in cols)]  # Adjust keys as needed
     errors = bigquery_client.insert_rows(table, row_to_insert)

     if errors == []:
          print("Row inserted successfully.")
     else:
          print("Error inserting row: {}".format(errors))

     # # Clean up: Delete the uploaded file
     # blob.delete()

     print("Process completed.")
