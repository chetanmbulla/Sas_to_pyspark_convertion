/* Tests macro parameters, defaults, %IF/%ELSE, %LET, and bounded %DO loops. */

%let default_minimum_age = 18;

%macro build_customer_report(
    input,
    output,
    minimum_age=&default_minimum_age
);
    %if &minimum_age ge 21 %then %let customer_segment=ADULT;
    %else %let customer_segment=YOUNG;

    data work.&output;
        set work.&input;
        where age ge &minimum_age;
        full_name = catx(' ', upcase(first_name), upcase(last_name));
        annual_spend = round(monthly_spend * 12, 0.01);
        segment = "&customer_segment";
        keep customer_id full_name age region monthly_spend annual_spend segment;
    run;

    %do period = 1 %to 2;
        data work.&output._period&period;
            set work.&output;
            reporting_period = &period;
        run;
    %end;
%mend build_customer_report;

/* Keyword arguments override the macro's default age. */
%build_customer_report(
    input=customers_macro,
    output=eligible_customers,
    minimum_age=21
);

/* Omitted minimum_age uses the default value of 18. */
%build_customer_report(
    customers_macro,
    default_age_customers
);

proc export data=work.eligible_customers
    outfile="eligible_customers.csv"
    dbms=csv
    putnames=yes;
run;

proc export data=work.default_age_customers
    outfile="default_age_customers.csv"
    dbms=csv
    putnames=yes;
run;
