from bson import ObjectId

from caladrius.DocumentDB import MongoClient
from cc_archive.archive_cc_rds import publish_archive_cc_rds

with MongoClient('master') as db_client:
    orders_collection = db_client['covidclinic']['orders']
    bad_orders = []
    i = 0
    for bad_order in orders_collection.find({'source_of_truth': 'woocommerce',
                                             'raw_body.line_items': {'$exists': False}}):
        print(f'Deleting {bad_order["mrn"]}')
        bad_orders.append((bad_order["mrn"], ObjectId(bad_order['_id'])))
        i += 1

        if i > 5:
            break

    delete_response = orders_collection.delete_many({'_id': {'$in': [bad_order[1] for bad_order in bad_orders]}})


def divide_chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


for mrns_left in divide_chunks([bad_order[0] for bad_order in bad_orders], 100):
    publish_archive_cc_rds(mrns_left)
