/* Level 4: merge two inputs, classify joined rows, and reshape with TRANSPOSE. */

data work.customer_orders;
    merge work.accounts_l4 work.orders_l4;
    by customer_id;
run;

data work.scored_orders;
    set work.customer_orders;
    where amount gt 0;

    commission = round(amount * commission_rate, 0.01);
    select;
        when (amount ge 1000) order_band = 'LARGE';
        when (amount ge 500) order_band = 'MEDIUM';
        otherwise order_band = 'SMALL';
    end;
    keep customer_id region transaction_id product amount commission order_band;
run;

proc sort data=work.scored_orders out=work.orders_sorted;
    by region descending amount;
run;

proc transpose data=work.orders_sorted out=work.region_order_metrics
    order=transaction_id;
    by region;
    var amount commission;
run;

proc freq data=work.scored_orders;
    tables region * order_band / out=work.order_frequency;
run;

proc sql;
    create table work.region_sales as
    select region,
           count(*) as order_count,
           sum(amount) as total_amount,
           sum(commission) as total_commission
    from work.scored_orders
    group by region;
quit;

proc export data=work.region_sales
    outfile="level4_region_sales.csv"
    dbms=csv
    putnames=yes;
run;
