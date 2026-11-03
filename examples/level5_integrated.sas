/* Level 5: macros, three-way key merge, arrays, classifications, and reports. */

%let service_fee = 25;
%let high_value = 1500;

data work.customer_activity;
    merge work.profiles_l5 work.subscriptions_l5 work.payments_l5;
    by customer_id;
run;

data work.customer_value;
    set work.customer_activity;
    where active_flag eq 1;

    array revenue_parts[2] subscription_fee payment_amount;
    total_value = revenue_parts[1] + revenue_parts[2] + &service_fee;
    annualized_value = round(total_value * 12, 0.01);

    if total_value ge &high_value then value_band = 'HIGH';
    else value_band = 'STANDARD';

    select;
        when (tenure_months ge 36) tenure_band = 'LONG';
        when (tenure_months ge 12) tenure_band = 'MID';
        otherwise tenure_band = 'NEW';
    end;

    display_name = catx(' ', upcase(first_name), upcase(last_name));
    keep customer_id display_name region plan total_value annualized_value
         value_band tenure_band;
run;

proc sort data=work.customer_value out=work.customer_value_sorted;
    by region descending total_value;
run;

proc freq data=work.customer_value_sorted;
    tables region * value_band / out=work.value_frequency;
run;

proc means data=work.customer_value_sorted n sum mean min max;
    class region;
    var total_value annualized_value;
    output out=work.region_value_metrics;
run;

proc sql;
    create table work.executive_summary as
    select region,
           count(*) as customer_count,
           sum(total_value) as monthly_value,
           avg(annualized_value) as average_annualized_value
    from work.customer_value_sorted
    group by region;
quit;

proc export data=work.executive_summary
    outfile="level5_executive_summary.csv"
    dbms=csv
    putnames=yes;
run;
