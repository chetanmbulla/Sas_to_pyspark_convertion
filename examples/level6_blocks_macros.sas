/* Level 6: IF without ELSE, ELSE IF, IF THEN DO blocks, DELETE, %EVAL, %SYSFUNC, %DO %WHILE, &&indirect. */

%let min_score = %eval(2 * 20);
%let region_label = %sysfunc(upcase(north));
%let part1 = gold;
%let part2 = silver;

data work.scored;
    set work.applicants_l6;

    /* IF without ELSE: keeps existing value (null for a new column). */
    if age lt 21 then minor_flag = 1;

    /* ELSE IF chain. */
    if score ge 90 then tier = 'A';
    else if score ge 75 then tier = 'B';
    else if score ge 60 then tier = 'C';
    else tier = 'D';

    /* IF THEN DO blocks. */
    if score ge 90 then do;
        reward = 'gold';
        bonus = 100;
    end;
    else if score ge 75 then do;
        reward = 'silver';
        bonus = 50;
    end;
    else do;
        reward = 'none';
        bonus = 0;
    end;

    /* THEN DELETE: rows with status X are removed; missing status is kept. */
    if status = 'X' then delete;

    /* Macro arithmetic and functions evaluated at conversion time. */
    passing = score ge &min_score;
    label_text = "&region_label";
    keep applicant_id region age score status minor_flag tier reward bonus passing label_text;
run;

/* %DO %WHILE and indirect &&part&i references create work.list_gold and work.list_silver. */
%let i = 1;
%do %while (&i le 2);
    data work.list_&&part&i;
        set work.scored;
        where reward = "&&part&i";
    run;
    %let i = %eval(&i + 1);
%end;

proc sql;
    create table work.tier_summary as
    select tier,
           count(*) as applicants,
           avg(score) as average_score,
           sum(bonus) as total_bonus
    from work.scored
    group by tier;
quit;

proc export data=work.scored
    outfile="level6_scored.csv"
    dbms=csv
    putnames=yes;
run;

proc export data=work.tier_summary
    outfile="level6_tier_summary.csv"
    dbms=csv
    putnames=yes;
run;
