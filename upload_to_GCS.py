import datetime
import os
import traceback

from google.cloud import storage

import COMMON


if __name__ == "__main__":

    started_time = datetime.datetime.now()

    project_path = '/home/nkh_nguyenhuy961127/DEC_PJ5_GCP/'
    logger = COMMON.get_log(project_path + 'log/upload.log')

    logger.info(f'STARTED TIME: {started_time}')
    print(f'STARTED TIME: {started_time}')

    # Config credentials file (json) as Enviroment Variable 'GOOGLE_APPLICATION_CREDENTIALS'
    # os.environ['GOOGLE_CLOUD_PROJECT'] = 'path-to-json-file'
    os.environ['GOOGLE_CLOUD_PROJECT'] = 'sage-mind-388000'
    bucket_name = 'test_script_nkh'
    try:
        full_path = '/home/nkh_nguyenhuy961127/DEC_PJ5_GCP/export/'
        fname = 'product.json'

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(fname)

        blob.chunk_size = 1024 * 1024 * 1024 * 50  # 50GB
        blob.upload_from_filename(full_path + fname, if_generation_match=0)

        logger.info(f"File [{full_path + fname}] uploaded to [{fname}] in bucket [{bucket_name}].")
        print(f"File [{full_path + fname}] uploaded to [{fname}] in bucket [{bucket_name}].")

        logger.info(f'FINISHED TIME: {datetime.datetime.now()}')
        print(f'FINISHED TIME: {datetime.datetime.now()}')
    except Exception as e:
        logger.error(traceback.format_exc())
