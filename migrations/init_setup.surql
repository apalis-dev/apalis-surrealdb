BEGIN TRANSACTION;

DEFINE NAMESPACE IF NOT EXISTS apalis; 
DEFINE DATABASE IF NOT EXISTS apalis;
USE NAMESPACE apalis DATABASE apalis; 

-- Workers Table
DEFINE TABLE IF NOT EXISTS workers SCHEMAFULL;
-- Workers Fields
DEFINE FIELD IF NOT EXISTS id            ON workers TYPE string;
DEFINE FIELD IF NOT EXISTS worker_type   ON workers TYPE string; 
DEFINE FIELD IF NOT EXISTS storage_name  ON workers TYPE string;
DEFINE FIELD IF NOT EXISTS layers        ON workers TYPE string;
DEFINE FIELD IF NOT EXISTS started_at    ON workers TYPE int;
DEFINE FIELD IF NOT EXISTS last_seen     ON workers TYPE int DEFAULT time::unix();

DEFINE INDEX IF NOT EXISTS Idx ON TABLE workers FIELDS id; 
DEFINE INDEX IF NOT EXISTS WTIdx ON TABLE workers FIELDS worker_type;
DEFINE INDEX IF NOT EXISTS LSIdx ON TABLE workers FIELDS last_seen;

-- Jobs Table
DEFINE TABLE IF NOT EXISTS jobs SCHEMAFULL;
-- Jobs Fields
DEFINE FIELD IF NOT EXISTS job          ON jobs TYPE bytes;
DEFINE FIELD IF NOT EXISTS id           ON jobs TYPE string;
DEFINE FIELD IF NOT EXISTS job_type     ON jobs TYPE string;
DEFINE FIELD IF NOT EXISTS status       ON jobs TYPE string DEFAULT 'Pending';
DEFINE FIELD IF NOT EXISTS attempts     ON jobs TYPE int DEFAULT 0;
DEFINE FIELD IF NOT EXISTS max_attempts ON jobs TYPE int DEFAULT 25;
DEFINE FIELD IF NOT EXISTS run_at       ON jobs TYPE int DEFAULT time::unix();
DEFINE FIELD IF NOT EXISTS last_result  ON jobs TYPE option<string>;
DEFINE FIELD IF NOT EXISTS lock_at      ON jobs TYPE option<int>;
DEFINE FIELD IF NOT EXISTS lock_by      ON jobs TYPE option<record<workers>>;
DEFINE FIELD IF NOT EXISTS done_at      ON jobs TYPE option<int>;
DEFINE FIELD IF NOT EXISTS priority     ON jobs TYPE int DEFAULT 0;
DEFINE FIELD IF NOT EXISTS metadata     ON jobs TYPE option<object>;

DEFINE INDEX IF NOT EXISTS Idx ON TABLE jobs FIELDS id; 
DEFINE INDEX IF NOT EXISTS SIdx ON TABLE jobs FIELDS status;
DEFINE INDEX IF NOT EXISTS LIdx ON TABLE jobs FIELDS lock_by;
DEFINE INDEX IF NOT EXISTS JTIdx ON TABLE jobs FIELDS job_type;

COMMIT TRANSACTION;