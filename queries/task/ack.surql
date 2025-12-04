LET $updated = UPDATE $task_id
                    SET 
                        status = $status,
                        attempts = $attempts,
                        last_result = $last_result,
                        done_at = time::unix()
                    WHERE lock_by = $worker_id
                    RETURN AFTER;

RETURN count($updated);