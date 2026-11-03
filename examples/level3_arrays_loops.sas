/* Level 3: indexed arrays, calculated columns, and an output-generating DO loop. */

data work.expanded_assessments;
    set work.assessments_l3;
    where score1 ge 0 and score2 ge 0 and score3 ge 0;

    array scores[3] score1-score3;
    total_score = scores[1] + scores[2] + scores[3];
    average_score = total_score / 3;

    select;
        when (average_score ge 85) result = 'DISTINCTION';
        when (average_score ge 70) result = 'PASS';
        otherwise result = 'REVIEW';
    end;

    do assessment_part = 1 to 3;
        part_score = total_score / 3;
        output;
    end;

    keep student_id course assessment_part total_score average_score
         result part_score;
run;

proc means data=work.expanded_assessments n mean min max;
    class course;
    var average_score part_score;
    output out=work.course_metrics;
run;

proc sql;
    create table work.course_summary as
    select course,
           count(*) as assessment_rows,
           avg(average_score) as mean_score
    from work.expanded_assessments
    group by course;
quit;

proc export data=work.course_summary
    outfile="level3_course_summary.csv"
    dbms=csv
    putnames=yes;
run;
