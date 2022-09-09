import os
from datetime import datetime as dt
from glob import glob
from os.path import join, getctime

from ELIS import ELISAgent
from Caladrius.auxiliary import hash_file
from Caladrius.billing import compute_billing_range


def pull_elis_billing(billing_ordinal: int):
    def at(filename):
        return join(r'/mnt/p//billing_reports', f'billing_{billing_ordinal}', filename)

    e_agent = ELISAgent()

    e_agent.login()
    start_date, end_date = compute_billing_range(billing_ordinal)
    print(f'Pulling ELIS billing report #{billing_ordinal} ({start_date.date()} -- {end_date.date()})')
    df = e_agent.get_extended_billing(start_date, end_date)
    df = df.rename(columns=lambda x: x.strip())
    df.rename({
        'Tertiary Insurance Address': 'Tertiary Insurance Claims Address',
        'Tertiary Date Of Birth': 'Tertiary Subscriber Date Of Birth'
    }, axis=1, inplace=True)
    this_elis_pull_path = at(rf'BillingExport_'
                             rf'{start_date.strftime("%Y%m%d")}--'
                             rf'{end_date.strftime("%Y%m%d")}_{dt.utcnow().strftime("%Y%m%dT%H%M%S")}.csv')
    df.to_csv(this_elis_pull_path, index=False)

    previous_elis_pull_paths = sorted(glob(at('BillingExport_*.csv')))
    previous_elis_pull_paths.remove(this_elis_pull_path)
    previous_elis_pull_hashes = [hash_file(p) for p in previous_elis_pull_paths]
    this_elis_pull_hash = hash_file(this_elis_pull_path)

    print(this_elis_pull_path)

    if this_elis_pull_hash in previous_elis_pull_hashes:
        print('Data already pulled')
        os.remove(this_elis_pull_path)
    else:
        print(f'Saved to {this_elis_pull_path}')


if __name__ == '__main__':
    # for ordinal in range(77, 100):
    #     pull_elis_billing(ordinal)
    pull_elis_billing(105)
