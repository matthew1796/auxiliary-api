import os
from glob import glob
from os import path
from os.path import join

import pandas as pd

pd.options.display.width = 0


####################################################
#  V V V V V  FOR COVID CLINIC REPORTS  V V V V V  #
####################################################
def generate_covid_clinic_billing(billing_ordinal: int):
    def at(filename):
        return join(r'/mnt/p/billing_reports', f'billing_{billing_ordinal}', filename)

    billing_report_paths = glob(at(f'Billing Report*.csv'))
    print(billing_report_paths)
    # billing_report_path = f'billing_{report_number}/Billing Report.csv'
    # if path.isfile(billing_report_path):
    if billing_report_paths:
        billing_report_paths.sort(key=lambda x: os.path.getmtime(x))

        billing_reports_cc = []
        for billing_report_path in billing_report_paths:
            print(f'Appending {billing_report_path}')
            billing_reports_cc.append(pd.read_csv(billing_report_path,  encoding= 'unicode_escape', dtype=str))
        # billing_report_cc = pd.read_csv(billing_report_path, names=header)
        billing_report_cc = pd.concat(billing_reports_cc)
        billing_report_cc['Date of Results'] = pd.to_datetime(billing_report_cc['Date of Results'])
        billing_report_cc['Date of Results'] = billing_report_cc['Date of Results'].dt.tz_localize('US/Pacific')
        billing_report_cc['Date of Collection'] = pd.to_datetime(billing_report_cc['Date of Collection'])
        billing_report_cc['Date of Collection'] = billing_report_cc['Date of Collection'].dt.tz_localize('US/Pacific')

        client_bill_paths = glob(at('Client Bill*.csv'))
        if client_bill_paths:
            client_bills = []
            for client_bill_path in client_bill_paths:
                cb = pd.read_csv(client_bill_path,  encoding= 'unicode_escape')
                client_bills.extend(cb['Accession Number'])

            l0 = len(billing_report_cc)
            billing_report_cc = billing_report_cc[
                ~billing_report_cc['Accession Number'].isin(client_bills)]
            print(f'Removed {l0 - len(billing_report_cc)} MRNs that are client bill')
        else:
            print(f'Directory "billing_{billing_ordinal}/" devoid of Client Bills')

        failed_path = at('Failed*.csv')
        if path.isfile(failed_path):
            failed = pd.read_csv(failed_path, names=['Date', 'MRN', 'Result'])
            l1 = len(billing_report_cc)
            billing_report_cc = billing_report_cc[~billing_report_cc['Accession Number'].isin(failed['MRN'] + '-09')]
            print(f'Removed {l1 - len(billing_report_cc)} MRNs that failed')
        else:
            print(f'There is no {failed_path} file')

        leaking_paths = glob(at('Leaking*.csv'))
        if leaking_paths:
            leakings = []
            for leaking_path in leaking_paths:
                leakings.append(pd.read_csv(leaking_path, names=['Date', 'MRN', 'Result']))
            leaking = pd.concat(leakings)
            l2 = len(billing_report_cc)
            billing_report_cc = billing_report_cc[~billing_report_cc['Accession Number'].isin(leaking['MRN'] + '-09')]
            print(f'Removed {l2 - len(billing_report_cc)} MRNs that are leaky')
        else:
            print(f'Directory "billing_{billing_ordinal}/" devoid of Leakings')

        duplicates = billing_report_cc[
            billing_report_cc.duplicated(subset=['Accession Number', 'Date of Collection'], keep='last')]
        l3 = len(billing_report_cc)
        billing_report_cc.drop_duplicates(subset=['Accession Number', 'Date of Collection'], keep='last', inplace=True)
        print(f'Removed {l3 - len(billing_report_cc)} MRNs that were duplicates')
        print(duplicates)

        no_dob_reports = billing_report_cc[pd.isna(billing_report_cc['Patient DOB'])]
        billing_report_cc.drop(no_dob_reports.index, inplace=True)
        print(f'Removed {len(no_dob_reports)} records that had no DOB')
        print(no_dob_reports)

        collection_timestamps_path = at('collection_timestamps.csv')
        if path.isfile(collection_timestamps_path):
            collection_timestamps_df = pd.read_csv(collection_timestamps_path)
            collection_timestamps_df['Collection Timestamp'] = pd.to_datetime(
                collection_timestamps_df['Collection Timestamp'])
            billing_report_cc['MRN'] = billing_report_cc['Accession Number'].str[:-3]

            billing_report_cc = pd.merge(billing_report_cc, collection_timestamps_df, how='left', on='MRN')
            billing_report_cc['Collection Timestamp'] = pd.to_datetime(billing_report_cc['Collection Timestamp'],
                                                                       utc=True).dt.tz_convert('US/Pacific')
            billing_report_cc.loc[pd.isna(billing_report_cc['Date of Collection']), 'Date of Collection'] = \
                billing_report_cc['Collection Timestamp']
            billing_report_cc['__TAT__'] = (
                    billing_report_cc['Date of Results'] - billing_report_cc['Date of Collection']).dt.days
            billing_report_cc['__U0005__'] = billing_report_cc['__TAT__'] <= 2
            billing_report_cc.loc[billing_report_cc['__U0005__'] & ~billing_report_cc['Billing CPT Code'].str.contains(
                'U0005'), 'Billing CPT Code'] += ' U0005'
            billing_report_cc.drop(labels=['MRN', 'Collection Timestamp', '__TAT__', '__U0005__'], axis='columns',
                                   inplace=True)

        #Per Matt Collins, Phil Dumas, and Dr Matthew Abinante on 09/08/2022, use Z20.828 for all orders with Matthew Abinante as the Ordering Provider.
        billing_report_cc['ICD 10 Code'] = 'Z20.828'
        # billing_report_cc.loc[
        #     ~billing_report_cc['Billing CPT Code'].str.contains('G2023'), 'Billing CPT Code'] += ' G2023'
        # Per Matt Collins as of 2022-03-15 we are removing G2023 from all Covid Clinic samples
        billing_report_cc.loc[:, 'Billing CPT Code'] = billing_report_cc.loc[:, 'Billing CPT Code'] \
            .str.replace('G2023', '')
        billing_report_cc.dropna(subset=['Date of Collection'], inplace=True)
        billing_report_cc['Date of Collection'] = billing_report_cc['Date of Collection'].dt.strftime('%m/%d/%Y')
        billing_report_cc['Date of Results'] = billing_report_cc['Date of Results'].dt.strftime('%m/%d/%Y')

        billing_report_path = at(f'MedLab Billing Report {billing_ordinal}.csv')
        billing_report_cc.to_csv(billing_report_path,
                                 index=False,
                                 encoding='utf-8')
        return billing_report_path
    else:
        print(f'Directory "billing_{billing_ordinal}/" devoid of Billing Reports')
