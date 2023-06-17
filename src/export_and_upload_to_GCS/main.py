import os
import sys
import json
import datetime
import subprocess
import traceback

import pymongo

import COMMON


def export_to_jsonl():

    start_time = datetime.datetime.now()
    export_logger = COMMON.get_log(project_path + 'log/export.log')
    out_put_category = open(export_file_category, 'w')
    out_put_product = open(export_file_product, 'w')

    try:
        export_logger.info("Start exporting at: %s" % datetime.datetime.now())

        # Connect to MongoDB
        mongo_server = pymongo.MongoClient(COMMON.MONGODB_LOCALHOST)
        mongo_db = mongo_server[COMMON.MONGODB_DB_NAME]
        mongo_coll_product = mongo_db["product"]
        mongo_coll_category = mongo_db["category"]
        export_logger.info("Export from: %s%s" % (COMMON.MONGODB_LOCALHOST, COMMON.MONGODB_DB_NAME))

        categories = mongo_coll_category.find({})
        categories_count = mongo_coll_category.count_documents()
        for cat in categories:
            cat['_id'] = str(cat['_id'])
            cat['crawled_time'] = str(cat['crawled_time'])
            out_put_category.write(json.dumps(cat) + "\n")
        export_logger.info('Exported: %i categories!' % categories_count)

        products = mongo_coll_product.find({})
        products_count = mongo_coll_product.count_documents()
        for prod in products:
            prod['_id'] = str(prod['_id'])
            prod['crawled_time'] = str(prod['crawled_time'])
            out_put_product.write(json.dumps(prod) + "\n")
        export_logger.info('Exported: %i products!' % products_count)
    except Exception as e:
        export_logger.error(e)
        export_logger.error(traceback.format_exc())
    finally:
        out_put_category.close()
        out_put_product.close()
        COMMON.print_execution_time(export_logger, start_time)


def upload_to_gcs(upload_src):

    start_time = datetime.datetime.now()
    upload_logger = COMMON.get_log(project_path + 'log/upload.log')

    try:
        upload_logger.info("Start uploading at: %s" % datetime.datetime.now())
        upload_logger.info("Uploading file %s to bucket %s" % (upload_src, bucket_name))

        # Using parallel when uploading more than 150MB
        cmd = 'gsutil -o GSUtil:parallel_composite_upload_threshold=150M -m -cp %s gs://%s' % (upload_src, bucket_name)
        # Execute Command
        result = subprocess.run(cmd, shell=True, bufsize=1, capture_output=True, text=True)
        if result.returncode != 0:
            upload_logger.error("UPLOAD FAILED")
            upload_logger.error(result.stderr)
        else:
            upload_logger.info("UPLOADED SUCCESSFULLY")
            upload_logger.info("File [%s] UPLOADED TO BUCKET [%s]." % (upload_src, bucket_name))
    except Exception as e:
        upload_logger.error(e)
        upload_logger.error(traceback.format_exc())
    finally:
        COMMON.print_execution_time(upload_logger, start_time)


if __name__ == '__main__':

    home_path = os.environ['HOME']
    project_path = home_path + '/PROJECT/DEC_PJ5_GCP/'

    bucket_name = sys.argv[1]

    export_file_category = project_path + 'export/category.json'
    export_file_product = project_path + 'export/product.json'
    # Export Data
    export_to_jsonl()

    # Upload file to Bucket (Overwrite if exist)
    # Upload Category Collection
    upload_to_gcs(export_file_category)
    # Upload Product Collection
    upload_to_gcs(export_file_product)
