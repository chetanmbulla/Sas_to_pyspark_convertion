/* Example covering arrays, SELECT, NODUPKEY, DO/OUTPUT, aggregates, and transpose. */

data work.scored_sales;
    set work.sales;
    where amount gt 0;

    array scores[3] score1-score3;
    average_score = (scores[1] + scores[2] + scores[3]) / 3;

    select;
        when (amount ge 1000) sale_band = 'HIGH';
        when (amount ge 500) sale_band = 'MEDIUM';
        otherwise sale_band = 'LOW';
    end;

    keep sale_id region quarter product amount average_score sale_band;
run;

proc sort data=work.scored_sales out=work.sales_unique;
    by sale_id;
run;

data work.installments;
    set work.sales_unique;
    do installment = 1 to 2;
        installment_amount = amount / 2;
        output;
    end;
    keep sale_id region installment installment_amount;
run;

proc means data=work.sales_unique n sum mean std min max;
    class region;
    var amount average_score;
    output out=work.region_statistics;
run;

proc freq data=work.sales_unique;
    tables sale_band / out=work.sale_band_frequency;
run;

proc transpose data=work.sales_unique out=work.sales_wide order=sale_id;
    by region;
    var amount average_score;
run;

proc sql;
    create table work.product_summary as
    select product,
           count(*) as sale_count,
           sum(amount) as total_amount,
           avg(average_score) as average_score
    from work.sales_unique
    group by product;
quit;

proc export data=work.product_summary
    outfile="product_summary.csv"
    dbms=csv
    putnames=yes;
run;
