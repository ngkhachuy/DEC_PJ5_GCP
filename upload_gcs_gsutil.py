import datetime
import os
import sys
import subprocess

import COMMON

if __name__ == '__main__':

    started_time = datetime.datetime.now()
    home_path = os.environ['HOME']
    project_path = home_path + '/PROJECT/DEC_PJ5_GCP/'
    logger = COMMON.get_log(project_path + 'log/upload.log')

    logger.info("STARTED TIME: %s" % started_time)

    cmd_action = ''
    upload_src = sys.argv[1]
    if os.path.isfile(upload_src):
        cmd_action = '-m cp'
        src_type = 'File'
    else:
        cmd_action = '-m cp -r'
        src_type = 'Folder'
    bucket_name = sys.argv[2]

    logger.info("Uploading %s %s to bucket %s" % (src_type, upload_src, bucket_name))

    # Using parallel when uploading more than 150MB
    cmd = 'gsutil -o GSUtil:parallel_composite_upload_threshold=150M %s %s gs://%s' % (cmd_action,
                                                                                       upload_src,
                                                                                       bucket_name)
    result = subprocess.run(cmd, shell=True, bufsize=1, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error("UPLOAD FAILED")
        logger.error(result.stderr)
    else:
        logger.info("UPLOADED SUCCESSFULLY")
        logger.info("%s [%s] UPLOADED TO BUCKET [%s]." % (src_type, upload_src, bucket_name))

    logger.info("FINISHED TIME: %s" % datetime.datetime.now())
