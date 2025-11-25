-- Get all unique job types from both Workers and Jobs
LET $all_job_types = array::distinct(
                                array::concat(
                                (SELECT VALUE worker_type FROM workers), 
                                (SELECT VALUE job_type FROM jobs)
                                ));

-- Calculate statistics for each job type
LET $stats = (
        SELECT
            job_type as name,
            [
                {
                    title: 'RUNNING_JOBS',
                    stat_type: 'Number',
                    value: string::concat(count(status[WHERE status = 'Running'])),
                    priority: 1
                },
                {
                    title: 'PENDIND_JOBS',
                    stat_type: 'Number',
                    value: string::concat(count(status[WHERE status = 'Pending'])),
                    priority: 1
                },
                {
                    title: 'FAILED_JOBS',
                    stat_type: 'Number',
                    value: string::concat(count(status[WHERE status = 'Failed'])),
                    priority: 1
                },
                {
                    title: 'ACTIVE_JOBS',
                    stat_type: 'Number',
                    value: string::concat(count(status[WHERE status IN ['Running', 'Queued', 'Pending']])),
                    priority: 2
                },
                {
                    title: 'STALE_RUNNING_JOBS',
                    stat_type: 'Number',
                    value: string::concat(count(run_at[WHERE status = 'Running' AND run_at < time::unix() - 3600])),
                    priority: 2
                },
                {
                    title: 'KILL_RATE',
                    stat_type: 'Percentage',
                    value: string::concat(math::fixed(count(status[WHERE status = 'Killed']) * 100.0)/ math::max(count(), 1), 2)),
                    priority: 2
                },
                {
                    title: 'JOBS_PAST_HOUR',
                    stat_type: 'Number',
                    value: string::concat(count(run_at[WHERE run_at >= time::unix() - 3600])),
                    priority: 3
                },
                {
                    title: 'JOBS_TODAY',
                    stat_type: 'Number',
                    value: string::concat(count(run_at[WHERE time::from_unix(run_at) >= time::floor(time::now(), 1d) AND time::from_unix(run_at) < time::ceil(time::now, 1d)])),
                    priority: 3
                },
                {
                    title: 'KILLED_JOBS_TODAY',
                    stat_type: 'Number',
                    value: string::concat(count(status[WHERE status = 'Killed' AND time::from_unix(run_at) >= time::floor(time::now(), 1d) AND time::from_unix(run_at) < time::ceil(time::now, 1d)])),
                    priority: 3
                },
                {
                    title: 'AVG_JOBS_PER_MINUTE_PAST_HOUR',
                    stat_type: 'Number',
                    value: string::concat(count(run_at[WHERE time::from_unix(run_at) >= time::floor(time::now(), 1d) AND time::from_unix(run_at) < time::ceil(time::now, 1d)])),
                    priority: 3
                },
                {
                    title: 'TOTAL_JOBS',
                    stat_type: 'Number',
                    value: string::concat(count()),
                    priority: 4
                },
                {
                    title: 'DONE_JOBS',
                    stat_type: 'Number',
                    value: string::concat(count(status[WHERE status = 'Done'])),
                    priority: 4 
                }
                {
                    title: 'KILLED_JOBS',
                    stat_type: 'Number',
                    value: string::concat(count(status[WHERE status = 'Killed'])),
                    priority: 4 
                },
                {
                    title: 'SUCCESS_RATE',
                    stat_type: 'Percentage',
                    value: string::concat(math::fixed((count(status[WHERE status = 'Done']) * 100.0) / math::max(count(), 1), 2)),
                    priority: 4 
                },
                {
                    title: 'AVG_JOB_DURATION_MINS',
                    stat_type: 'Decimal',
                    value: string::concat(
                            math::fixed(
                                math::mean(
                                (SELECT VALUE (done_at - run_at) / 60 FROM jobs
                                        WHERE job_type = $parent.job_type
                                        AND status IN ['Done', 'Failed', 'Killed']
                                        AND done_at IS NOT NONE)
                                ),
                            2)),
                    priority: 5
                },
                {
                    title: 'LONGEST_RUNNING_JOB_MINS',
                    stat_type: 'Decimal',
                    value: string::concat(
                            math::fixed(
                                math::max(
                                    (SELECT VALUE (time::unix() - run_at) / 60 FROM jobs
                                        WHERE job_type = $parent.job_type
                                        AND status = 'Running')
                                ) ?? 0
                            , 2)
                            ),
                    priority: 5
                },
                {
                    title: 'JOBS_PAST_7_DAYS',
                    stat_type: 'Number',
                    value: string::concat(count(run_at[WHERE time::from_unix(run_at) >= time::now() - 7d])),
                    priority: 6
                },
                {
                    title: 'MOST_RECENT_JOB',
                    stat_type: 'Timestamp',
                    value: string::concat(math::max(time::from_unix(run_at))),
                    priority: 8
                }
            ] as stats
        FROM jobs
        GROUP BY job_type
    );

RETURN array::map($all_job_types, |$jt| {
    LET $job_stats = (SELECT VALUE stats FROM $stats WHERE name = $jt LIMIT 1)[0] ?? [];

    LET $workers = array::distinct((SELECT VALUE lock_by FROM jobs
                                        WHERE job_type = $jt
                                        AND lock_by IS NOT NONE));
    
    LET $activity = (SELECT VALUE count FROM (
                        SELECT count() AS count FROM jobs 
                            WHERE job_type = $jt
                            AND time::from_unix(run_at) >= time::now() - 7d
                            GROUP BY time::group(time::from_unix(run_at), 'day')
                            ORDER BY time::group(time::from_unix(run_at), 'day')
                        ));
    RETURN {
            name: $jt,
            stats: $job_stats,
            workers: $workers,
            activity: $activity}
});