SELECT * FROM jobs 
            WHERE status = $status 
            AND job_type = $job_type 
            ORDER BY done_at DESC, run_at DESC
            LIMIT $limit
            OFFSET $offset";