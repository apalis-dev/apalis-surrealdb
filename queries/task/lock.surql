LET $updated = UPDATE $task_id
                    SET 
                        status = 'Running',
                        lock_at = time::unix(),
                        lock_by = $worker_id
                    WHERE
                        status = 'Queued' 
                        OR status = 'Pending' 
                        OR (status = 'Failed' AND attempts < max_attempts)
                    RETURN AFTER;

RETURN count($updated);