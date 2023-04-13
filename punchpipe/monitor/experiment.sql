SELECT COUNT(flow_id) from flows WHERE state = 'ended' AND TIMEDIFF(end_time, start_time) > 1;

SELECT flow_type, COUNT(flow_id) from flows
    WHERE state = 'ended'
        AND TIMEDIFF(end_time, start_time) < 3
        AND start_time > '2022-05-08T10:00:00'
    GROUP BY flow_type;


# fast and slow flows with universal limit
SELECT flow_kind_descr,
       CASE
            WHEN num_slow IS NULL THEN 0
            WHEN num_slow IS NOT NULL THEN num_slow
       END AS frequency
    FROM
        (SELECT flow_kind, COUNT(flow_kind) as num_slow
            FROM flows
            WHERE state = 'ended'
                AND TIMEDIFF(end_time, start_time) < 3
                AND start_time > '2022-05-08T10:00:00'
                AND end_time < '2022-06-20T00:00:00'
            GROUP BY flow_kind)
    AS T RIGHT JOIN flow_kinds ON flow_kind = flow_kind_id;


# fast and slow flows with flow type limit
SELECT flow_kind_descr,
       CASE
            WHEN num_slow IS NULL THEN 0
            WHEN num_slow IS NOT NULL THEN num_slow
       END AS frequency
    FROM
        (SELECT flow_kind, COUNT(flow_kind) as num_slow
            FROM flows JOIN flow_kinds ON flow_kind = flow_kind_id
            WHERE state = 'ended'
                AND TIMEDIFF(end_time, start_time) < fast_threshold
                AND start_time > '2022-05-08T10:00:00'
                AND end_time < '2022-06-20T00:00:00'
            GROUP BY flow_kind)
    AS T RIGHT JOIN flow_kinds ON flow_kind = flow_kind_id;

# average duration and std dev duration
SELECT flow_kind_descr,
       CASE
           WHEN avg_duration IS NULL THEN 0
           WHEN avg_duration IS NOT NULL THEN avg_duration
        END AS duration,
        CASE
            WHEN std_duration IS NULL THEN 0
            WHEN std_duration IS NOT NULL THEN std_duration
        END AS deviation
    FROM
        (SELECT flow_kind,
                AVG(TIMEDIFF(end_time, start_time)) as avg_duration,
                STDDEV(TIMEDIFF(end_time, start_time)) as std_duration
            FROM flows JOIN flow_kinds ON flow_kind = flow_kind_id
            WHERE state = 'ended'
                AND start_time > '2022-05-08T10:00:00'
                AND end_time < '2022-06-20T00:00:00'
            GROUP BY flow_kind)
    AS T RIGHT JOIN flow_kinds ON flow_kind = flow_kind_id;

# number of files recently written
SELECT flow_kind_descr,
       CASE
           WHEN count_files IS NULL THEN 0
           WHEN count_files IS NOT NULL THEN count_files
        END AS num_files
    FROM
        (SELECT flow_kind,
                COUNT(FILE_ID) as count_files
            FROM
                files JOIN flows ON processing_flow = flow_id JOIN flow_kinds ON flow_kind = flow_kind_id
            WHERE date_acquired > '2022-05-08T10:00:00'
                AND date_acquired < '2022-06-20T00:00:00'
            GROUP BY flow_kind)
    AS T RIGHT JOIN flow_kinds ON flow_kind = flow_kind_id;

# count the number of recently written files
SELECT flow_kind_descr,
       CASE
           WHEN count_files IS NULL THEN 0
           WHEN count_files IS NOT NULL THEN count_files
        END AS num_files
    FROM
        (SELECT flow_kind,
                COUNT(file_id) as count_files
            FROM
                flows JOIN flow_kinds ON flow_kind = flow_kind_id JOIN files ON processing_flow = flow_id
            WHERE date_acquired > '2022-05-08T10:00:00'
                AND date_acquired < '2022-06-20T00:00:00'
            GROUP BY flow_kind)
    AS T RIGHT JOIN flow_kinds ON flow_kind = flow_kind_id;
