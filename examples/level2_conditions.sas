/* Level 2: macros, filtering, conditional assignments, functions, and procedures. */

%let minimum_age = 21;
%let bonus_rate = 0.05;
%let premium_spend = 1000;

data work.customer_segments;
    set work.customer_spend_l2;
    where age ge &minimum_age and not missing(monthly_spend);

    annual_bonus = round(monthly_spend * &bonus_rate, 1);
    if monthly_spend ge &premium_spend then segment = 'PREMIUM';
    else segment = 'STANDARD';

    select;
        when (age ge 60) age_band = 'SENIOR';
        when (age ge 40) age_band = 'MID';
        otherwise age_band = 'JUNIOR';
    end;

    keep customer_id region age monthly_spend annual_bonus segment age_band;
run;

proc freq data=work.customer_segments;
    tables region * segment / out=work.segment_frequency;
run;

proc means data=work.customer_segments n sum mean min max;
    class region;
    var monthly_spend annual_bonus;
    output out=work.region_metrics;
run;

proc sql;
    create table work.segment_summary as
    select segment,
           count(*) as customer_count,
           avg(monthly_spend) as average_spend
    from work.customer_segments
    group by segment;
quit;

proc export data=work.segment_summary
    outfile="level2_segment_summary.csv"
    dbms=csv
    putnames=yes;
run;
