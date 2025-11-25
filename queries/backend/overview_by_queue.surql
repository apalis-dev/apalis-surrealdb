-- START
LET $all_count = (SELECT count() AS count FROM jobs WHERE job_type = $job_type GROUP ALL)[0].count ?? 0;

LET $all_count_24h = (SELECT count() FROM jobs WHERE job_type = $job_type AND time::from::unix(run_at) >= time::now() - 1d GROUP ALL)[0].count ?? 0;

LET $all_count_7d = (SELECT count() FROM jobs WHERE job_type = $job_type AND time::from::unix(run_at) >= time::now() - 7d GROUP ALL)[0].count ?? 0;

LET $killed_count = (SELECT count() AS count FROM jobs WHERE job_type = $job_type AND status = 'Killed' GROUP ALL)[0].count ?? 0;

LET $kill_rate = math::fixed($killed_count * 100.0 / $all_count, 2) ?? 0;

LET $success_count = (SELECT count() AS count FROM jobs WHERE job_type = $job_type AND status = 'Done' GROUP ALL)[0].count ?? 0;

LET $success_rate = math::fixed($success_count * 100.0 / $all_count, 2) ?? 0;


LET $success_count_24h = (SELECT count() AS count FROM jobs WHERE job_type = $job_type AND status = 'Done' AND time::from::unix(run_at) >= time::now() - 1d GROUP ALL)[0].count ?? 0;

LET $success_rate_24h = math::fixed($success_count_24h * 100.0 / $all_count_24h, 2) ?? 0;


RETURN [
    {
        priority: 1,
        type: 'Number',
        statistic: 'RUNNING_JOBS',
        value: (SELECT count() FROM jobs WHERE job_type = $job_type AND status = 'Running' GROUP ALL)[0].count ?? 0
    },
    {
        priority: 1,
        type: 'Number',
        statistic: 'PENDING_JOBS',
        value: (SELECT VALUE count() FROM jobs WHERE job_type = $job_type AND status = 'Pending' GROUP ALL)[0].count ?? 0
    },
    {
        priority: 2,
        type: 'Number',
        statistic: 'FAILED_JOBS',
        value: (SELECT VALUE count() FROM jobs WHERE job_type = $job_type AND status = 'Failed' GROUP ALL)[0].count ?? 0
    },
    {
        priority: 2,
        type: 'Number',
        statistic: 'ACTIVE_JOBS',
        value: (SELECT VALUE count() FROM jobs WHERE job_type = $job_type AND status IN ['Pending', 'Running', 'Queued'] GROUP ALL)[0].count ?? 0
    },
    {
        priority: 2,
        type: 'Number',
        statistic: 'STALE_RUNNING_JOBS',
        value: (SELECT VALUE count() FROM jobs WHERE job_type = $job_type AND status = 'Running' AND time::from::unix(run_at) < time::now() - 1h GROUP ALL)[0].count ?? 0
    },
    {
        priority: 2,
        type: 'Percentage',
        statistic: 'KILL_RATE',
        value: $kill_rate,
    },
    {
        priority: 3,
        type: 'Number',
        statistic: 'JOBS_PAST_HOUR',
        value: (SELECT VALUE count() FROM jobs WHERE job_type = $job_type AND time::from::unix(run_at) >= time::now() - 1h GROUP ALL)[0].count ?? 0
    },
    {
        priority: 3,
        type: 'Number',
        statistic: 'JOBS_TODAY',
        value: (SELECT VALUE count() FROM jobs WHERE job_type = $job_type AND time::group(time::from::unix(run_at), 'day') = time::group(time::now(), 'day') GROUP ALL)[0].count ?? 0
    },
    {
        priority: 3,
        type: 'Number',
        statistic: 'KILLED_JOBS_TODAY',
        value: (SELECT VALUE count() FROM jobs WHERE job_type = $job_type AND status = 'Killed' AND time::group(time::from::unix(run_at), 'day') = time::group(time::now(), 'day') GROUP ALL)[0].count ?? 0
    },
    {
        priority: 3,
        type: 'Decimal',
        statistic: 'AVG_JOBS_PER_MINUTE_PAST_HOUR',
        value: math::fixed(
            (SELECT VALUE count() / 60.0 FROM jobs WHERE job_type = $job_type AND time::from::unix(run_at) >= time::now() - 1h GROUP ALL)[0].count ?? 0,
            2
        )
    },
    {
        priority: 4,
        type: 'Number',
        statistic: 'TOTAL_JOBS',
        value: $all_count
    },
    {
        priority: 4,
        type: 'Number',
        statistic: 'DONE_JOBS',
        value: $success_count
    },
    {
        priority: 4,
        type: 'Number',
        statistic: 'COMPLETED_JOBS',
        value: (SELECT VALUE count() FROM jobs WHERE job_type = $job_type AND status IN ['Done', 'Failed', 'Killed'] GROUP ALL)[0].count ?? 0
    },
    {
        priority: 4,
        type: 'Number',
        statistic: 'KILLED_JOBS',
        value: $killed_count
    },
    {
        priority: 4,
        type: 'Percentage',
        statistic: 'SUCCESS_RATE',
        value: $success_rate
    },
    {
        priority: 5,
        type: 'Decimal',
        statistic: 'AVG_JOB_DURATION_MINS',
        value: math::fixed((SELECT math::mean((done_at - run_at)/60.0) AS mean FROM jobs WHERE job_type = $job_type AND status IN ['Done', 'Failed', 'Killed'] AND done_at IS NOT NONE GROUP ALL)[0].mean, 2)
    },
    {
        priority: 5,
        type: 'Decimal',
        statistic: 'LONGEST_RUNNING_JOB_MINS',
        value: math::fixed(math::max(SELECT VALUE (time::unix() - run_at) / 60.0 FROM jobs WHERE job_type = $job_type AND status = 'Running'), 2)
    },
    {
        priority: 5,
        type: 'Number',
        statistic: 'QUEUE_BACKLOG',
        value: (SELECT VALUE count() FROM jobs WHERE job_type = $job_type AND status = 'Pending' AND time::from::unix(run_at) <= time::now() GROUP ALL)[0].count ?? 0
    },
    {
        priority: 6,
        type: 'Number',
        statistic: 'JOBS_PAST_24_HOURS',
        value: (SELECT VALUE count() FROM jobs WHERE job_type = $job_type AND time::from::unix(run_at) >= time::now() - 1d GROUP ALL)[0].count ?? 0
    },
    {
        priority: 6,
        type: 'Number',
        statistic: 'JOBS_PAST_7_DAYS',
        value: (SELECT VALUE count() FROM jobs WHERE job_type = $job_type AND time::from::unix(run_at) >= time::now() - 7d GROUP ALL)[0].count ?? 0
    },
        {
        priority: 6,
        type: 'Number',
        statistic: 'KILLED_JOBS_PAST_7_DAYS',
        value: (SELECT VALUE count() FROM jobs WHERE job_type = $job_type AND time::from::unix(run_at) >= time::now() - 7d AND status = 'Killed' GROUP ALL)[0].count ?? 0
    },
    {
        priority: 6,
        type: 'Percentage',
        statistic: 'SUCCESS_RATE_PAST_24H',
        value: $success_rate_24h
    },
    {
        priority: 7,
        type: 'Decimal',
        statistic: 'AVG_JOBS_PER_HOUR_PAST_24H',
        value: math::fixed($all_count_24h / 24.0, 2)
    },
    {
        priority: 7,
        type: 'Decimal',
        statistic: 'AVG_JOBS_PER_DAY_PAST_7D',
        value: math::fixed($all_count_7d / 7.0, 2)
    },
    {
        priority: 8,
        type: 'Timestamp',
        statistic: 'MOST_RECENT_JOB',
        value: (SELECT time::max(time::from::unix(run_at)) AS max_time FROM jobs WHERE job_type = $job_type GROUP ALL)[0].max_time ?? time::now()
    },
    {
        priority: 8,
        type: 'Timestamp',
        statistic: 'OLDEST_PENDING_JOB',
        value: (SELECT time::min(time::from::unix(run_at)) AS min_time FROM jobs WHERE job_type = $job_type AND status = 'Pending' AND run_at <= time::unix() GROUP ALL)[0].min_time ?? time::now()
    },
    {
        priority: 8,
        type: 'Number',
        statistic: 'PEAK_HOUR_JOBS',
        value: (SELECT time::max(hour) AS peak_jobs FROM (SELECT hour, count() FROM (SELECT time::group(time::from::unix(run_at), 'hour') AS hour FROM jobs WHERE job_type = $job_type AND time::from::unix(run_at) >= time::now() - 1d) GROUP BY hour) GROUP ALL)[0].peak_jobs ?? 0
    }
];