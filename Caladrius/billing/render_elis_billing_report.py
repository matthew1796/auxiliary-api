from collections import defaultdict, OrderedDict
from glob import glob
from os.path import getctime
from os.path import join

import pandas as pd
from pandas import Series

pd.options.display.width = 0

insurance_stages = ['Primary', 'Secondary', 'Tertiary']

nary_insurance_pairing = [('# Insurance Name', '# Insurance Name'),
                          ('# Payer of Insurance First Name', 'Guarantor First Name'),
                          ('# Payer of Insurance Last Name', 'Guarantor Last Name'),
                          ('# Insurance ID Number', '# Insurance Policy Number'),
                          ('# Insurance Group Number', '# Insurance Group Number'),
                          ('# Subscriber Name', '__# Subscriber Name__'),
                          ('# Subscriber DOB', '# Subscriber Date Of Birth')]
ranked_insurance_pairing = []
for stage in insurance_stages:
    for x, y in nary_insurance_pairing:
        ranked_insurance_pairing.append((x.replace('#', stage), y.replace('#', stage)))

mpp_from_elis = OrderedDict(
    [('Patient First Name', 'Patient First Name'),
     ('Patient Last Name', 'Patient Last Name'),
     ('Patient Street', 'Patient Address'),
     ('Patient City', 'Patient City'),
     ('Patient State', 'Patient State'),
     ('Patient Zip', 'Patient Zip'),
     ('Patient DOB', 'Patient Date Of Birth'),
     ('Patient Sex', '__Patient Sex__'),
     ('Patient Phone Number', 'Patient Home Phone (including area code)'),
     *ranked_insurance_pairing,
     ('Ordering Provider', 'Ordering Physician First Name, Last Name, Middle'),
     ('Ordering Provider NPI', 'Ordering Doctor NPI Code'),
     ('Accession Number', 'Medical Record # (MRN)'),
     ('Facility Street', '__Facility Street__'),
     ('Facility City', '__Facility City__'),
     ('Facility State', '__Facility State__'),
     ('Facility Zip', '__Facility Zip__'),
     ('Date of Collection', 'Date Of Service (aka Collection of Specimen Date)'),
     ('Date of Results', 'Resulted Date/Interpretation Date'),
     ('Billing CPT Code', 'Numeric CPT Code'),
     ('ICD 10 Code', 'Admitting Diagnosis code(s)'),
     ('Facility ID', '__Facility ID__'),
     ('Patient Drivers License', '__Patient Drivers License__'),
     ('Patient Social Security Number', 'Patient SSN')])


mpp_from_elis_with_specID = OrderedDict(
    [('Patient First Name', 'Patient First Name'),
     ('Patient Last Name', 'Patient Last Name'),
     ('Patient Street', 'Patient Address'),
     ('Patient City', 'Patient City'),
     ('Patient State', 'Patient State'),
     ('Patient Zip', 'Patient Zip'),
     ('Patient DOB', 'Patient Date Of Birth'),
     ('Patient Sex', '__Patient Sex__'),
     ('Patient Phone Number', 'Patient Home Phone (including area code)'),
     *ranked_insurance_pairing,
     ('Ordering Provider', 'Ordering Physician First Name, Last Name, Middle'),
     ('Ordering Provider NPI', 'Ordering Doctor NPI Code'),
     ('Accession Number', 'Medical Record # (MRN)'),
     ('Facility Street', '__Facility Street__'),
     ('Facility City', '__Facility City__'),
     ('Facility State', '__Facility State__'),
     ('Facility Zip', '__Facility Zip__'),
     ('SpecimenID', 'Specimen ID'),
     ('Date of Collection', 'Date Of Service (aka Collection of Specimen Date)'),
     ('Date of Results', 'Resulted Date/Interpretation Date'),
     ('Billing CPT Code', 'Numeric CPT Code'),
     ('ICD 10 Code', 'Admitting Diagnosis code(s)'),
     ('Facility ID', '__Facility ID__'),
     ('Patient Drivers License', '__Patient Drivers License__'),
     ('Patient Social Security Number', 'Patient SSN')])


def generate_elis_report(billing_ordinal: int):
    def at(filename):
        return join(r'/mnt/p/billing_reports', f'billing_{billing_ordinal}', filename)

    elis_billing_exports = glob(at('BillingExport_*.csv'))
    if not elis_billing_exports:
        print('No ELIS billing export found')
    else:
        print(f'Rendering ELIS report # {billing_ordinal - 36}')

        billing_reports_elis = [pd.read_csv(elis_billing_export, dtype=str)
                                for elis_billing_export in sorted(elis_billing_exports, key=getctime)]
        # billing_report_elis = pd.concat(billing_reports_elis)
        # elis_billing_exports = sorted(elis_billing_exports, key=getctime, reverse=True)
        # df = pd.read_csv(elis_billing_exports[0], dtype=str)
        df = pd.concat(billing_reports_elis, ignore_index=True)
        df.drop_duplicates(inplace=True, keep='last')

        # cc_records = df.loc[df['Ordering Doctor NPI Code'] == '1740685387'].index
        cc_records = df.loc[df['Account Name'] == 'Covid Clinic'].index
        df.drop(cc_records, inplace=True)
        if len(cc_records):
            print(f'Dropped {len(cc_records)} Covid Clinic tests')

        sex_map = defaultdict(lambda: '')
        sex_map['M'] = 'Male'
        sex_map['F'] = 'Female'
        df['__Patient Sex__'] = df['Patient Sex'].apply(lambda s: sex_map[s])

        # x = [stage + ' Insurance Name' for stage in insurance_stages]

        elis_insurance_info = ['Insurance Code', 'Insurance Name', 'Insurance Claims Address', 'Insurance City',
                               'Insurance State', 'Insurance Zip', 'Subscriber First Name', 'Subscriber Last Name',
                               'Subscriber Middle Initial', 'Subscriber Relationship to Patient',
                               'Insurance Policy Number', 'Insurance Group Number', 'Subscriber Date Of Birth',
                               'Subscriber Sex', 'Subscriber SSN', 'Authorization Number']

        ranked_insurance_names = [x + ' Insurance Name' for x in insurance_stages]

        # df.apply(my_func, axis=1, result_type='expand')
        # row_1 = df.loc[0]
        # row_12 = df.loc[215]
        # row_2 = df.loc[216]

        def swap_stages(row: Series, stage_a, stage_b):
            swap_a_to_b = {f'{stage_a} {info}': f'{stage_b} {info}' for info in elis_insurance_info}
            swap_b_to_a = {f'{stage_b} {info}': f'{stage_a} {info}' for info in elis_insurance_info}
            swap_a_to_b.update(swap_b_to_a)
            return row.rename(swap_a_to_b)

        def evaluate_stage_quality(stage_insurance_name: str):
            is_stage_medi_cal = 'MEDI-CAL' in stage_insurance_name.upper() \
                                or 'MEDICAL' == stage_insurance_name.upper().strip()
            if stage_insurance_name:
                if not is_stage_medi_cal:
                    return 10
                else:
                    return 5
            else:
                return 0

        def my_func(row):
            for _ in range(len(insurance_stages)):
                for j in range(len(insurance_stages) - 1):
                    stage_j = insurance_stages[j]
                    stage_k = insurance_stages[j + 1]

                    stage_j_insurance_name = row.fillna('')[f'{stage_j} Insurance Name']
                    stage_k_insurance_name = row.fillna('')[f'{stage_k} Insurance Name']

                    j_quality = evaluate_stage_quality(stage_j_insurance_name)
                    k_quality = evaluate_stage_quality(stage_k_insurance_name)

                    if j_quality < k_quality:
                        row = swap_stages(row, stage_j, stage_k)
                # print(f'Loop {i}: ')
                # print(row[ranked_insurance_names])
                # print('############')
            return row

        # print(row_2[ranked_insurance_names])
        # row_2 = my_func(row_2)
        # print(row_2[ranked_insurance_names])
        # df = df.loc[[0, 215, 216], :].apply(my_func, axis=1)
        print('Ranking and sorting insurances...')
        df = df.apply(my_func, axis=1)
        print('Insurances ranked and sorted.')
        df = df.copy()
        for stage in insurance_stages:
            df[f'__{stage} Subscriber Name__'] = df[f'{stage} Subscriber First Name'].fillna('') + ' ' + df[
                f'{stage} Subscriber Last Name'].fillna('')

        # billing_report_elis['__Ordering Provider__'] = 'Albert Lai'
        # billing_report_elis['__Ordering Provider NPI__'] = ''
        df['__Facility Street__'] = ''
        df['__Facility City__'] = ''
        df['__Facility State__'] = ''
        df['__Facility Zip__'] = ''
        df['__Facility ID__'] = ''
        df['__Patient Drivers License__'] = ''

        df['Date Of Service (aka Collection of Specimen Date)'] = pd.to_datetime(
            df['Date Of Service (aka Collection of Specimen Date)'], errors='coerce')
        df['Resulted Date/Interpretation Date'] = pd.to_datetime(
            df['Resulted Date/Interpretation Date'], errors='coerce')

        df['__TAT__'] = (
                df['Resulted Date/Interpretation Date'] -
                df['Date Of Service (aka Collection of Specimen Date)']).dt.days
        df['__U0005__'] = df['__TAT__'] <= 2
        df.loc[df['__U0005__'], 'Numeric CPT Code'] += ' U0005'

        elis_out = pd.DataFrame()
        for k, v in mpp_from_elis.items():
            elis_out[k] = df[v]

        elis_out['Date of Collection'] = elis_out['Date of Collection'].dt.strftime('%m/%d/%Y')
        elis_out['Date of Results'] = elis_out['Date of Results'].dt.strftime('%m/%d/%Y')

        no_z_code = elis_out[~elis_out['ICD 10 Code'].fillna('').str.contains('Z')].index
        elis_out.drop(no_z_code, inplace=True)
        if len(no_z_code):
            print(f'Dropped {len(no_z_code)} with no Z Code as the first')

        no_npi_code = elis_out[elis_out['Ordering Provider NPI'].fillna('').str.len() == 0].index
        elis_out.drop(no_npi_code, inplace=True)
        if len(no_npi_code):
            print(f'Dropped {len(no_npi_code)} with no Ordering Provider NPI')

        has_at_least_secondary = elis_out[elis_out['Secondary Insurance Name'].fillna('').str.len() > 0]

        elis_pst_path = at(f'MedLab ELIS Billing Report {billing_ordinal - 36} PST.csv')
        elis_out.loc[has_at_least_secondary.index, :].to_csv(elis_pst_path, index=False)
        elis_p_only_path = at(f'MedLab ELIS Billing Report {billing_ordinal - 36} P Only.csv')
        elis_out_p_only = elis_out.loc[
            elis_out.index.difference(has_at_least_secondary.index), [c for c in elis_out.columns
                                                                      if ('Secondary' not in c) and (
                                                                              'Tertiary' not in c)]]
        elis_out_p_only.to_csv(elis_p_only_path, index=False)

        return elis_pst_path



def generate_elis_report_with_specID(billing_ordinal: int):
    def at(filename):
        return join(r'/mnt/p/billing_reports/archive', f'billing_{billing_ordinal}', filename)



    print(at('BillingExport_*.csv'))
    elis_billing_exports = glob(at('BillingExport_*.csv'))
    if not elis_billing_exports:
        print('No ELIS billing export found')
    else:
        print(f'Rendering ELIS report # {billing_ordinal - 36} with SpecimenID')

        billing_reports_elis = [pd.read_csv(elis_billing_export, dtype=str)
                                for elis_billing_export in sorted(elis_billing_exports, key=getctime)]
        # billing_report_elis = pd.concat(billing_reports_elis)
        # elis_billing_exports = sorted(elis_billing_exports, key=getctime, reverse=True)
        # df = pd.read_csv(elis_billing_exports[0], dtype=str)
        df = pd.concat(billing_reports_elis, ignore_index=True)
        df.drop_duplicates(inplace=True, keep='last')

        # cc_records = df.loc[df['Ordering Doctor NPI Code'] == '1740685387'].index
        cc_records = df.loc[df['Account Name'] == 'Covid Clinic'].index
        df.drop(cc_records, inplace=True)
        if len(cc_records):
            print(f'Dropped {len(cc_records)} Covid Clinic tests')

        sex_map = defaultdict(lambda: '')
        sex_map['M'] = 'Male'
        sex_map['F'] = 'Female'
        df['__Patient Sex__'] = df['Patient Sex'].apply(lambda s: sex_map[s])

        # x = [stage + ' Insurance Name' for stage in insurance_stages]

        elis_insurance_info = ['Insurance Code', 'Insurance Name', 'Insurance Claims Address', 'Insurance City',
                               'Insurance State', 'Insurance Zip', 'Subscriber First Name', 'Subscriber Last Name',
                               'Subscriber Middle Initial', 'Subscriber Relationship to Patient',
                               'Insurance Policy Number', 'Insurance Group Number', 'Subscriber Date Of Birth',
                               'Subscriber Sex', 'Subscriber SSN', 'Authorization Number']

        ranked_insurance_names = [x + ' Insurance Name' for x in insurance_stages]

        # df.apply(my_func, axis=1, result_type='expand')
        # row_1 = df.loc[0]
        # row_12 = df.loc[215]
        # row_2 = df.loc[216]

        def swap_stages(row: Series, stage_a, stage_b):
            swap_a_to_b = {f'{stage_a} {info}': f'{stage_b} {info}' for info in elis_insurance_info}
            swap_b_to_a = {f'{stage_b} {info}': f'{stage_a} {info}' for info in elis_insurance_info}
            swap_a_to_b.update(swap_b_to_a)
            return row.rename(swap_a_to_b)

        def evaluate_stage_quality(stage_insurance_name: str):
            is_stage_medi_cal = 'MEDI-CAL' in stage_insurance_name.upper() \
                                or 'MEDICAL' == stage_insurance_name.upper().strip()
            if stage_insurance_name:
                if not is_stage_medi_cal:
                    return 10
                else:
                    return 5
            else:
                return 0

        def my_func(row):
            for _ in range(len(insurance_stages)):
                for j in range(len(insurance_stages) - 1):
                    stage_j = insurance_stages[j]
                    stage_k = insurance_stages[j + 1]

                    stage_j_insurance_name = row.fillna('')[f'{stage_j} Insurance Name']
                    stage_k_insurance_name = row.fillna('')[f'{stage_k} Insurance Name']

                    j_quality = evaluate_stage_quality(stage_j_insurance_name)
                    k_quality = evaluate_stage_quality(stage_k_insurance_name)

                    if j_quality < k_quality:
                        row = swap_stages(row, stage_j, stage_k)
                # print(f'Loop {i}: ')
                # print(row[ranked_insurance_names])
                # print('############')
            return row

        # print(row_2[ranked_insurance_names])
        # row_2 = my_func(row_2)
        # print(row_2[ranked_insurance_names])
        # df = df.loc[[0, 215, 216], :].apply(my_func, axis=1)
        print('Ranking and sorting insurances...')
        df = df.apply(my_func, axis=1)
        print('Insurances ranked and sorted.')
        df = df.copy()
        for stage in insurance_stages:
            df[f'__{stage} Subscriber Name__'] = df[f'{stage} Subscriber First Name'].fillna('') + ' ' + df[
                f'{stage} Subscriber Last Name'].fillna('')

        # billing_report_elis['__Ordering Provider__'] = 'Albert Lai'
        # billing_report_elis['__Ordering Provider NPI__'] = ''
        df['__Facility Street__'] = ''
        df['__Facility City__'] = ''
        df['__Facility State__'] = ''
        df['__Facility Zip__'] = ''
        df['__Facility ID__'] = ''
        df['__Patient Drivers License__'] = ''

        df['Date Of Service (aka Collection of Specimen Date)'] = pd.to_datetime(
            df['Date Of Service (aka Collection of Specimen Date)'], errors='coerce')
        df['Resulted Date/Interpretation Date'] = pd.to_datetime(
            df['Resulted Date/Interpretation Date'], errors='coerce')

        df['__TAT__'] = (
                df['Resulted Date/Interpretation Date'] -
                df['Date Of Service (aka Collection of Specimen Date)']).dt.days
        df['__U0005__'] = df['__TAT__'] <= 2
        df.loc[df['__U0005__'], 'Numeric CPT Code'] += ' U0005'

        elis_out = pd.DataFrame()
        for k, v in mpp_from_elis_with_specID.items():
            elis_out[k] = df[v]

        elis_out['Date of Collection'] = elis_out['Date of Collection'].dt.strftime('%m/%d/%Y')
        elis_out['Date of Results'] = elis_out['Date of Results'].dt.strftime('%m/%d/%Y')

        no_z_code = elis_out[~elis_out['ICD 10 Code'].fillna('').str.contains('Z')].index
        elis_out.drop(no_z_code, inplace=True)
        if len(no_z_code):
            print(f'Dropped {len(no_z_code)} with no Z Code as the first')

        no_npi_code = elis_out[elis_out['Ordering Provider NPI'].fillna('').str.len() == 0].index
        elis_out.drop(no_npi_code, inplace=True)
        if len(no_npi_code):
            print(f'Dropped {len(no_npi_code)} with no Ordering Provider NPI')

        has_at_least_secondary = elis_out[elis_out['Secondary Insurance Name'].fillna('').str.len() > 0]

        elis_pst_path = at(f'MedLab ELIS Billing Report {billing_ordinal - 36} PST with SpecimenID.csv')
        elis_out.loc[has_at_least_secondary.index, :].to_csv(elis_pst_path, index=False)
        elis_p_only_path = at(f'MedLab ELIS Billing Report {billing_ordinal - 36} P Only with SpecimenID.csv')
        elis_out_p_only = elis_out.loc[
            elis_out.index.difference(has_at_least_secondary.index), [c for c in elis_out.columns
                                                                      if ('Secondary' not in c) and (
                                                                              'Tertiary' not in c)]]
        elis_out_p_only.to_csv(elis_p_only_path, index=False)

        return elis_pst_path


if __name__ == '__main__':
    for elis_report_i in range(103, 104):
        generate_elis_report(elis_report_i)
        # p_in = join(r'P:\billing_reports',
        #             f'billing_{elis_report_i}',
        #             f'MedLab ELIS Billing Report {elis_report_i - 36}.csv')
        # elis_df = pd.read_csv(p_in, dtype=str)
        # elis_df = elis_df.loc[:, [c for c in elis_df.columns if ('Secondary' not in c) and ('Tertiary' not in c)]]
        # # elis_df.
        # p_out = join(r'P:\billing_reports',
        #              f'billing_{elis_report_i}',
        #              f'MedLab ELIS Billing Report {elis_report_i - 36} P Only.csv')
        # elis_df.to_csv(p_out, index=False)
        # print()
