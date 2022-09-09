from Caladrius.billing.pull_elis_billing import pull_elis_billing
from Caladrius.billing.render_caladrius_billing_report import generate_covid_clinic_billing
from Caladrius.billing.render_elis_billing_report import generate_elis_report
from Caladrius.billing.render_elis_billing_report import generate_elis_report_with_specID
from Caladrius.billing  import compute_billing_range
# billing_ordinal = compute_current_billing_number()
billing_ordinal = 114

def generate_billing_reports(billing_ordinal : int):

    pull_elis_billing(billing_ordinal)
    print('-----')
    print(compute_billing_range(billing_ordinal))
    generate_covid_clinic_billing(billing_ordinal)
    print('-----')
    generate_elis_report(billing_ordinal)
    #generate_elis_report_with_specID(billing_ordinal)
