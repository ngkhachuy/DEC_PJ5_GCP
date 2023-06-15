import datetime
import os
import sys
import traceback

from google.cloud import storage

import COMMON


if __name__ == "__main__":

    started_time = datetime.datetime.now()
    home_path = os.environ['HOME']
    project_path = home_path + '/PROJECT/DEC_PJ5_GCP/'
    logger = COMMON.get_log(project_path + 'log/upload.log')

    logger.info(f'STARTED TIME: {started_time}')
    print(f'STARTED TIME: {started_time}')

    try:
        # Config credentials file (json) as Enviroment Variable 'GOOGLE_APPLICATION_CREDENTIALS'
        # os.environ['GOOGLE_CLOUD_PROJECT'] = 'path-to-json-file'
        os.environ['GOOGLE_CLOUD_PROJECT'] = 'sage-mind-388000'
        bucket_name = 'test_script_nkh'

        now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        with open('txt/test_%s.txt' % now, 'w') as f:
            f.write(now)

        fname = 'txt/test_%s.txt' % now

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(fname.split("/")[-1])

        blob.chunk_size = 1024 * 1024 * 1024 * 50  # 50GB
        blob.upload_from_filename(fname, if_generation_match=0)

        logger.info(f"File [{fname}] uploaded to bucket [{bucket_name}].")
        print(f"File [{fname}] uploaded to bucket [{bucket_name}].")

        logger.info(f'FINISHED TIME: {datetime.datetime.now()}')
        print(f'FINISHED TIME: {datetime.datetime.now()}')
    except Exception as e:
        logger.error(traceback.format_exc())
