/* Level 7: date literals, date functions, PUT/INPUT and FORMAT. */

data work.order_dates;
    set work.orders_l7;
    format order_date date9.;

    order_dt = input(order_text, yymmdd10.);
    order_year = year(order_dt);
    order_quarter = qtr(order_dt);
    order_weekday = weekday(order_dt);

    days_since_start = intck('day', '01JAN2024'd, order_dt);
    months_since_start = intck('month', '01JAN2024'd, order_dt);

    month_start = intnx('month', order_dt, 0, 'begin');
    month_end = intnx('month', order_dt, 0, 'end');
    due_date = intnx('day', order_dt, 30);

    order_label = put(order_dt, date9.);
    order_iso = put(order_dt, yymmdd10.);
    invoice_no = put(invoice_id, z6.);
    amount_text = put(amount, 10.2);

    if due_date lt '31MAR2024'd then due_bucket = 'Q1';
    else due_bucket = 'LATER';

    keep order_id order_text order_dt order_year order_quarter order_weekday
         days_since_start months_since_start month_start month_end due_date
         order_label order_iso invoice_no amount_text due_bucket;
run;

proc export data=work.order_dates
    outfile="level7_order_dates.csv"
    dbms=csv
    putnames=yes;
run;
