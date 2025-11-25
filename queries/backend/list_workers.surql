SELECT * FROM workers
            ORDER BY last_seen DESC
            LIMIT $limit
            OFFSET $offset