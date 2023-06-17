from google.cloud import bigquery
import functions_framework


@functions_framework.cloud_event
def load_table_uri_json(cloud_event):

    bucket_name = cloud_event.data["bucket"]
    generation = cloud_event.data["generation"]
    file_name = cloud_event.data["name"]
    file_path = cloud_event.data["id"].replace(generation, '')

    if file_name.endswith('.json'):

        # Construct a BigQuery client object.
        client = bigquery.Client()
        # ID of the table to create.
        table_id = "sage-mind-388000.TIKI_test_function.%s" % file_name.replace('.json', '')

        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            max_bad_records=50000
        )

        uri = "gs://%s" % file_path

        load_job = client.load_table_from_uri(
            uri,
            table_id,
            location="US",  # Must match the destination dataset location.
            job_config=job_config,
        )  # Make an API request.

        load_job.result()  # Waits for the job to complete.

        destination_table = client.get_table(table_id)
        print("Loaded {} rows.".format(destination_table.num_rows))
