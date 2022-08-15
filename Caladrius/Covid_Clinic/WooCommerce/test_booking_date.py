from dateutil import parser, tz


def get_amelia_appointment_datetime(order_json: dict):
    line_items = order_json.get('line_items')
    if line_items is None or len(line_items) == 0:
        raise ValueError('No line items---cannot find ameliabooking data')

    first_line_item_metadata = {x['key']: x['value'] for x in line_items[0]['meta_data']}
    ameliabooking = first_line_item_metadata.get('ameliabooking')

    if ameliabooking is None:
        raise ValueError('No ameliabooking data found in the first line item metadata')

    booking_end_naive_dt_str = ameliabooking.get('bookingEnd')
    booking_end_tz_str = ameliabooking.get('timeZone')

    if not booking_end_naive_dt_str:
        raise ValueError('No booking-end time')
    if not booking_end_tz_str:
        raise ValueError('No booking-end timezone')

    try:
        booking_end_naive_dt = parser.parse(booking_end_naive_dt_str)
    except parser.ParserError:
        raise ValueError(f'Could not parse "{booking_end_naive_dt_str}" as a datetime')

    booking_end_tz = tz.gettz(booking_end_tz_str)
    if not booking_end_tz:
        raise ValueError(f'"{booking_end_tz_str}" does not return a timezone')

    return booking_end_naive_dt.replace(tzinfo=booking_end_tz).astimezone(tz.gettz('utc'))


if __name__ == '__main__':
    from caladrius.Covid_Clinic import get_order

    print(get_amelia_appointment_datetime(get_order('00-3589586').json()))
