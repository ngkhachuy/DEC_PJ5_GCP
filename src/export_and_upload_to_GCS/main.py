import os
import sys
import json
import datetime
import subprocess

import pymongo

import COMMON


def export_to_jsonl():

    export_logger = COMMON.get_log(project_path + 'log/export.log')
    export_logger.info("Start exporting at: %s" % datetime.datetime.now())

    # Connect to MongoDB
    mongo_server = pymongo.MongoClient(COMMON.MONGODB_LOCALHOST)
    mongo_db = mongo_server[COMMON.MONGODB_DB_NAME]
    mongo_coll_product = mongo_db["product"]
    mongo_coll_category = mongo_db["category"]
    export_logger.info("Export from: %s%s" % (COMMON.MONGODB_LOCALHOST, COMMON.MONGODB_DB_NAME))

    categories = mongo_coll_category.find({})
    op_category = open('export/category_%s.json' % start_time.strftime("%Y%m%d%H%M%S"), 'a')
    for cat in categories:
        cat['_id'] = str(cat['_id'])
        cat['crawled_time'] = str(cat['crawled_time'])
        op_category.write(json.dumps(cat) + "\n")
    export_logger.info('Exported: %i categories!' % len(list(categories)))
    op_category.close()

    products = mongo_coll_product.find({})
    op_product = open('export/product_%s.json' % start_time.strftime("%Y%m%d%H%M%S"), 'a')
    for prod in products:
        prod['_id'] = str(prod['_id'])
        prod['crawled_time'] = str(prod['crawled_time'])
        op_product.write(json.dumps(prod) + "\n")
    export_logger.info('Exported: %i products!' % len(list(products)))
    op_product.close()

    COMMON.print_execution_time(export_logger, start_time)


def upload_to_gcs(upload_src, dest_file):

    upload_logger = COMMON.get_log(project_path + 'log/upload.log')
    upload_logger.info("Start uploading at: %s" % datetime.datetime.now())
    upload_logger.info("Uploading file %s to bucket %s" % (upload_src, dest_file))

    # Using parallel when uploading more than 150MB
    cmd = 'gsutil -o GSUtil:parallel_composite_upload_threshold=150M -m -cp %s %s' % (upload_src, dest_file)
    # Execute Command
    result = subprocess.run(cmd, shell=True, bufsize=1, capture_output=True, text=True)
    if result.returncode != 0:
        upload_logger.error("UPLOAD FAILED")
        upload_logger.error(result.stderr)
    else:
        upload_logger.info("UPLOADED SUCCESSFULLY")
        upload_logger.info("File [%s] UPLOADED TO BUCKET [%s]." % (upload_src, dest_file))

    COMMON.print_execution_time(upload_logger, start_time)


if __name__ == '__main__':

    start_time = datetime.datetime.now()
    home_path = os.environ['HOME']
    project_path = home_path + '/PROJECT/DEC_PJ5_GCP/'

    bucket_name = sys.argv[1]

    # Export Data
    export_to_jsonl()

    # Upload file to Bucket (Overwrite if exist)
    # Upload Category Collection
    upload_to_gcs('%s/export/category_%s.json' % (project_path, start_time.strftime("%Y%m%d%H%M%S")),
                  'gs://%s/category.json' % bucket_name)
    # Upload Product Collection
    upload_to_gcs('%s/export/product_%s.json' % (project_path, start_time.strftime("%Y%m%d%H%M%S")),
                  'gs://%s/product.json' % bucket_name)
