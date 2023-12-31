import datetime
import json

import pymongo


if __name__ == '__main__':

    execution_time = datetime.datetime.now()

    MONGODB_LOCALHOST = "mongodb://localhost:27017/"
    MONGODB_DB_NAME = "TIKI_NEW"

    # ------------------------------ Connect to MongoDB
    mongo_server = pymongo.MongoClient(MONGODB_LOCALHOST)
    mongo_db = mongo_server[MONGODB_DB_NAME]
    mongo_coll_product = mongo_db["product"]
    mongo_coll_category = mongo_db["category"]

    categories = mongo_coll_category.find({})
    op_category = open('export/category_%s.json' % execution_time.strftime("%Y%m%d%H%M%S"), 'a')
    cnt_cat = 0
    for cat in categories:
        cat['_id'] = str(cat['_id'])
        cat['crawled_time'] = str(cat['crawled_time'])
        op_category.write(json.dumps(cat) + "\n")
        cnt_cat += 1
        print('Exported: %i' % cnt_cat, end='\r')
    print('Exported: %i categories!' % cnt_cat)
    op_category.close()

    products = mongo_coll_product.find({})
    op_product = open('export/product_%s.json' % execution_time.strftime("%Y%m%d%H%M%S"), 'a')
    cnt_prod = 0
    for prod in products:
        prod['_id'] = str(prod['_id'])
        prod['crawled_time'] = str(prod['crawled_time'])
        op_product.write(json.dumps(prod) + "\n")
        cnt_prod += 1
        print('Exported: %i' % cnt_prod, end='\r')
    print('Exported: %i products!' % cnt_prod)
    op_product.close()
