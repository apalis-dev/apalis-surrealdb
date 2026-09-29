-- Define the new worker data
LET $new_worker = {
        id: $worker_id,
        worker_type: $queue,
        storage_name: $storage_name,
        layers: $layers,
        last_seen: time::unix(),
        started_at: time::unix()
    };

-- Get existing worker
LET $existing = (SELECT * FROM $worker_id);

IF !$existing OR !$existing.last_seen OR (time::unix() - $existing.last_seen[0]) >= $keep_alive {
    LET $updated = UPSERT $worker_id CONTENT $new_worker RETURN AFTER;

    RETURN count($updated);
}