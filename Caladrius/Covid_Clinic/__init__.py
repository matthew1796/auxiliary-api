import json
import re
from os import path

import requests as req
from woocommerce import API

from .ccadmin import LoginUtil

loginutil = LoginUtil()

cc_mrn_pattern = re.compile(r'^(\d{2})-(\d+)(?:-(\d+))?$')

source_woocommerce = 'woocommerce'
source_ccadmin = 'ccadmin'
# _source_elis_ = 'elis'

with open(path.join(path.dirname(__file__), 'wc_api_keys.json'), 'r') as api_key_filepath:
    api_keys = json.load(api_key_filepath)
    wc_api = {stage: {prefix: API(url=creds['url'],
                                  consumer_key=creds['key'],
                                  consumer_secret=creds['secret'],
                                  version="wc/v3",
                                  timout=20,
                                  query_string_auth=True,
                                  wp_api=True)
                      for prefix, creds in api_keys[stage].items()}
              for stage in api_keys.keys()}


def get_order(mrn: str, stage='production', source=None, notes=False):
    if mrn:
        x = mrn.split('-')
        if len(x) == 2:
            website_code = x[0]
            order_number = x[1]

            if not source:
                if website_code in wc_api[stage].keys():
                    source = source_woocommerce
                elif website_code == '03':
                    source = source_ccadmin

            if order_number:
                if source == source_woocommerce:
                    order = wc_api[stage][website_code].get("orders/" + order_number + ('/notes' if notes else ''))
                    order.close()
                    return order
                elif source == source_ccadmin:
                    token = loginutil.get_token(60 * 12)
                    with req.Session() as sesh:
                        sesh.headers.update({'Authorization': 'Bearer {}'.format(token)})
                        order_request = sesh.get('https://api.covidclinic.org/api/v1/storefront',
                                                 params={'limit': 1, 'mrn': mrn})
                        order_request.close()
                        return order_request
                else:
                    raise KeyError(f'Unrecognized source: "{source}"')
    return None


def get_site_keys(stage='production'):
    return wc_api[stage].keys()


def parse_metadata(order: json) -> dict:
    return {x['key']: x['value'] for x in order['meta_data']}


class CC_MRN:
    def __init__(self, cc_mrn: str):
        self.mrn_match = cc_mrn_pattern.match(cc_mrn)
        if self.mrn_match:
            self.site_code = self.mrn_match.group(1)
            self.order_id = self.mrn_match.group(2)
            self.specimen_code = self.mrn_match.group(3)
        else:
            self.site_code = None
            self.order_id = None
            self.specimen_code = None

    def to_str(self):
        if self.mrn_match:
            return self.mrn_match.group(0)
        else:
            return None

    def __bool__(self):
        return bool(self.mrn_match)


if __name__ == '__main__':
    while True:
        input_mrn = input('Enter the MRN: ')
        if input_mrn:
            try:
                my_order = get_order(input_mrn)
                if my_order.json():
                    print(json.dumps(my_order.json(), indent=4))

            except Exception as e:
                print(e)
        else:
            break
