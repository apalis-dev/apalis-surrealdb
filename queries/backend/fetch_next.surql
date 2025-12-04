LET $queue = array::map(SELECT id, priority, run_at FROM jobs
WHERE job_type =$v_job_type
AND (status = 'Pending' AND lock_by IS NONE) OR 
    (status = 'Failed' AND attempts < max_attempts)
AND (run_at IS NONE OR time::from::unix(run_at) < time::now())
ORDER BY priority DESC, run_at ASC, id ASC
LIMIT $v_job_count, |$v|$v.id);

UPDATE jobs
    SET status = 'Queued',
        lock_by = $worker_id,
        lock_at = time::unix()
    WHERE id IN $queue
RETURN AFTER;

