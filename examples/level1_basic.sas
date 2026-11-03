/* Level 1: filter rows, create a column, sort, and summarize. */

data work.adult_customers;
    set work.customers_l1;
    where age ge 18;
    full_name = catx(' ', upcase(first_name), upcase(last_name));
    keep customer_id full_name age region annual_spend;
run;

proc sort data=work.adult_customers out=work.adult_customers_sorted;
    by region descending annual_spend;
run;

proc sql;
    create table work.region_summary as
    select region,
           count(*) as customer_count,
           sum(annual_spend) as total_spend
    from work.adult_customers_sorted
    group by region;
quit;

proc export data=work.region_summary
    outfile="level1_region_summary.csv"
    dbms=csv
    putnames=yes;
run;
