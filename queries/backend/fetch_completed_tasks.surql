SELECT id, status, last_result AS result                             
    FROM jobs
        WHERE id IN $job_ids
        AND (status = 'Done' OR (status = 'Failed' AND attempts >= max_attempts) OR status = 'Killed');