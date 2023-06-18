from google.cloud import bigquery
import functions_framework


@functions_framework.cloud_event
def load_table_uri_json(cloud_event):

    generation = cloud_event.data["generation"]
    file_name = cloud_event.data["name"]
    file_path = cloud_event.data["id"].replace('/' + generation, '')

    if file_name.endswith('.json'):

        # Uploaded file to GCS
        uri = "gs://%s" % file_path

        # Connect to BigQuery
        client = bigquery.Client()
        # ID of the Destination Table
        table_id = "sage-mind-388000.TIKI_test_function.%s" % file_name.replace('.json', '')

        print("Load data from %s to Table %s" % (file_name, table_id))

        job_config = bigquery.LoadJobConfig(autodetect=True,
                                            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                                            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                                            max_bad_records=50000)

        # Create Request API to load table
        load_job = client.load_table_from_uri(uri, table_id, location="us-central1",  job_config=job_config)
        # Waits for the job to complete.
        load_job.result()

        destination_table = client.get_table(table_id)
        print("Loaded %s rows to table %s." % (destination_table.num_rows, table_id))
