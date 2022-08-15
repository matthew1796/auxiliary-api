import pandas as pd
from sqlalchemy import sql
import re
from caladrius.SQL import SQLAgent
from cc_archive.archive_cc_rds import publish_archive_cc_rds

with SQLAgent('production').get_connection('covid_clinic') as conn:
    print('Grabbing distinct MRNs from results table...')
    to_do = set(pd.read_sql(sql.text('SELECT DISTINCT(mrn) FROM resulting.results;'), conn).mrn)
    print('Grabbing distinct MRNs from rejections table...')
    to_do = to_do.union(set(pd.read_sql(sql.text('SELECT DISTINCT(mrn) FROM resulting.rejections;'), conn).mrn))

    print('Grabbing distinct MRNs from accessions table...')
    to_do = to_do.union(set(pd.read_sql(sql.text('SELECT DISTINCT(mrn) FROM resulting.accessions;'), conn).mrn))

    print('Grabbing distinct MRNs from billing...')
    already_done = set(pd.read_sql(sql.text('SELECT DISTINCT(mrn) FROM covid_clinic.billing'), conn).mrn)
    print('Grabbing distinct MRNs from patients...')
    already_done = already_done.intersection(
        set(pd.read_sql(sql.text('SELECT DISTINCT(mrn) FROM covid_clinic.patients'), conn).mrn))
    # print('Grabbing distinct MRNs from symptoms...')
    # already_done = already_done.intersection(
    #     set(pd.read_sql(sql.text('SELECT DISTINCT(mrn) FROM covid_clinic.symptoms'), conn).mrn))
    print('Grabbing distinct MRNs from orders...')
    already_done = already_done.intersection(
        set(pd.read_sql(sql.text('SELECT DISTINCT(mrn) FROM covid_clinic.orders'), conn).mrn))

    mrn_pattern = re.compile(r'0[0234]-[0-9]+')

    to_do = {x for x in to_do if mrn_pattern.match(x)}
    already_done = {x for x in already_done if mrn_pattern.match(x)}


def divide_chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


for mrns_left in divide_chunks(list(to_do - already_done), 100):
    publish_archive_cc_rds(mrns_left)
    # break
