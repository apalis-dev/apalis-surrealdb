LET $deleted =  DELETE jobs 
                    WHERE status = 'Done' 
                    OR status = 'KILLED' 
                    OR (status = 'Failed' AND max_attempts <= attempts) 
                    RETURN BEFORE;

RETURN count($deleted);