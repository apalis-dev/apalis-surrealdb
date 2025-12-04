LET $updated = UPDATE $worker_id 
                    SET last_seen = time::unix() 
                WHERE worker_type = $queue 
                RETURN AFTER;

RETURN count($updated);