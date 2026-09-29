LET $updated = UPDATE 
                    jobs 
                SET 
                    status = 'Pending', 
                    done_at = null, 
                    lock_by = null, 
                    lock_at = null,
                    attempts = attempts + 1, 
                    last_error= "{'Err': 'Re-enqueued due to worker heartbeat timeout'}"
                WHERE (status='Running' OR status = 'Queued') 
                    AND lock_by != NONE
                    AND lock_by.last_seen != NONE
                    AND (time::unix() - lock_by.last_seen) >= $dead_for
                    AND lock_by.worker_type = $worker_type
                RETURN AFTER;

RETURN count($updated);