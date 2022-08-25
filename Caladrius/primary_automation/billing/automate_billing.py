from primary_automation.billing.pull_elis_billing import pull_elis_billing
from render_caladrius_billing_report import generate_covid_clinic_billing
from render_elis_billing_report import generate_elis_report
from primary_automation.billing import compute_billing_range
billing_ordinal = compute_current_billing_number()


pull_elis_billing(billing_ordinal)
print('-----')
generate_covid_clinic_billing(billing_ordinal)
print('-----')
generate_elis_report(billing_ordinal)
