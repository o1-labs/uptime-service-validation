CREATE TABLE IF NOT EXISTS bot_logs (
	id SERIAL PRIMARY KEY, 
	processing_time DOUBLE PRECISION, 
	files_processed INT, 
	file_timestamps TIMESTAMPTZ(6), 
	batch_start_epoch BIGINT, 
	batch_end_epoch BIGINT
);

CREATE TABLE IF NOT EXISTS statehash (
	id SERIAL PRIMARY KEY,
	value TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_state_hash ON statehash USING btree (value);

CREATE TABLE IF NOT EXISTS bot_logs_statehash (
	id SERIAL PRIMARY KEY,
	parent_statehash_id INT, 
	statehash_id INT, 
	weight INT NOT NULL, 
	bot_log_id INT,
	CONSTRAINT fk_parent_statehash
		FOREIGN KEY(parent_statehash_id) 
		REFERENCES statehash(id),
	CONSTRAINT fk_statehash
		FOREIGN KEY(statehash_id) 
		REFERENCES statehash(id),
	CONSTRAINT fk_bot_log
		FOREIGN KEY(bot_log_id) 
		REFERENCES bot_logs(id)
);

CREATE TABLE IF NOT EXISTS nodes (
	id SERIAL PRIMARY KEY,
	block_producer_key TEXT,
	updated_at TIMESTAMPTZ(6),
	score INT,
	score_percent NUMERIC(5,2),
	discord_id TEXT,
	email_id TEXT,
	application_status BOOLEAN
);

-- The points table stores the points of each node for each validated submission.
CREATE TABLE IF NOT EXISTS points (
	id SERIAL PRIMARY KEY,
	file_name TEXT,
	file_timestamps TIMESTAMPTZ(6), 
	blockchain_epoch BIGINT, 
	blockchain_height BIGINT,
    amount INT, 
	created_at TIMESTAMPTZ(6),
	node_id INT,
	bot_log_id INT,
	statehash_id INT,
	CONSTRAINT fk_nodes
		FOREIGN KEY(node_id) 
		REFERENCES nodes(id),
	CONSTRAINT fk_bot_log
		FOREIGN KEY(bot_log_id) 
		REFERENCES bot_logs(id),
	CONSTRAINT fk_statehashes
		FOREIGN KEY(statehash_id) 
		REFERENCES statehash(id)
);

CREATE TABLE IF NOT EXISTS score_history (
	id SERIAL PRIMARY KEY,
	node_id INT,
	score_at TIMESTAMP(6), 
	score INT, 
	score_percent NUMERIC(5,2),
	CONSTRAINT fk_nodes
		FOREIGN KEY(node_id) 
		REFERENCES nodes(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_sh_node_score_at ON score_history USING btree (node_id, score_at);

-- Table submission is used to store the submissions made by each submitter.
CREATE TABLE IF NOT EXISTS submissions (
	-- filled by uptime_service_backend
    id SERIAL PRIMARY KEY,
    submitted_at_date DATE NOT NULL,
    submitted_at TIMESTAMP NOT NULL,
    submitter TEXT NOT NULL,
    created_at TIMESTAMP,
    block_hash TEXT,
    remote_addr TEXT,
    peer_id TEXT,
    snark_work BYTEA,
    graphql_control_port INT,
    built_with_commit_sha TEXT,
	-- filled by zk-validator component
    state_hash TEXT,
    parent TEXT,
    height INTEGER,
    slot INTEGER,
    validation_error TEXT,
    verified BOOLEAN
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_submissions_submitter_date ON submissions USING btree (submitter, submitted_at);

-- Additional indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_submissions_submitter_date ON submissions (submitter, submitted_at_date);
CREATE INDEX IF NOT EXISTS idx_submissions_submitter_datetime ON submissions (submitter, submitted_at DESC);

-- Table creation for points_summary
-- The points_summary table aggregates data related to node scoring.
-- It is designed to hold a unique combination of bot_log_id and node_id.
-- This allows us to ensure that each node's contribution within a single batch
-- is counted once, preventing the final score from exceeding 100% in cases
-- where block producers may send more than one valid submission in a batch.
-- It is used by `update_scoreboard` function to update the final_score of each node.
CREATE TABLE IF NOT EXISTS points_summary (
    bot_log_id INT NOT NULL,
    node_id INT NOT NULL
);

-- Unique index for points_summary
-- A unique index on bot_log_id and node_id ensures that the pair is unique within the table.
-- This supports the trigger function's role in preventing duplicate summary entries
-- for the same bot_log and node combination.
CREATE UNIQUE INDEX IF NOT EXISTS uq_ps_bot_log_node ON points_summary USING btree (bot_log_id, node_id);

-- Trigger function definition for updating the points_summary
-- This function, fn_update_point_summary, is a trigger function that responds to insert operations.
-- When a new point is inserted, this function attempts to insert a corresponding entry into
-- the points_summary table. If an entry with the same bot_log_id and node_id already exists,
-- it does nothing (avoids duplicate entries), ensuring that each node's score is calculated
-- correctly for a batch and that the final_score does not exceed 100%.
CREATE OR REPLACE FUNCTION fn_update_point_summary()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    INSERT INTO points_summary(bot_log_id, node_id)
    VALUES(NEW.bot_log_id, NEW.node_id)
    ON CONFLICT (bot_log_id, node_id) DO NOTHING;
    RETURN new;
END;
$function$;

-- Trigger for points_summary update
-- This trigger, trg_after_insert_points, is set to fire after a new record is inserted into the points table.
-- For each new row, it calls the fn_update_point_summary function to maintain the points_summary.
-- This mechanism is crucial for ensuring that each node's score within a batch is accounted for accurately.
CREATE OR REPLACE TRIGGER trg_after_insert_points
AFTER INSERT ON points
FOR EACH ROW
EXECUTE FUNCTION fn_update_point_summary();

-- Function to clean up old data
CREATE OR REPLACE FUNCTION cleanup_old_data(days_ago INTEGER) 
RETURNS void AS $$
DECLARE 
  threshold_epoch BIGINT;
  threshold_date DATE;
  points_summary_deleted INT;
  points_deleted INT;
  bot_logs_statehash_deleted INT;
  bot_logs_deleted INT;
  submissions_deleted INT;
  statehash_deleted INT;
BEGIN
  -- Calculate the threshold based on the input parameter
  SELECT EXTRACT(EPOCH FROM (NOW() - INTERVAL '1 day' * days_ago))::BIGINT INTO threshold_epoch;
  SELECT (NOW() - INTERVAL '1 day' * days_ago)::DATE INTO threshold_date;
  RAISE NOTICE 'Cleaning up data older than % days ago', days_ago;
  RAISE NOTICE 'Threshold epoch: %', threshold_epoch;
  RAISE NOTICE 'Threshold date: %', threshold_date;

  -- Delete from points_summary and count rows deleted
  WITH del AS (
    DELETE FROM points_summary
    WHERE bot_log_id IN (SELECT id FROM bot_logs WHERE batch_start_epoch < threshold_epoch)
    RETURNING 1
  )
  SELECT COUNT(*) INTO points_summary_deleted FROM del;
  RAISE NOTICE 'Deleted % rows from points_summary', points_summary_deleted;

  -- Delete from points and count rows deleted
  WITH del AS (
    DELETE FROM points
    WHERE bot_log_id IN (SELECT id FROM bot_logs WHERE batch_start_epoch < threshold_epoch)
    RETURNING 1
  )
  SELECT COUNT(*) INTO points_deleted FROM del;
  RAISE NOTICE 'Deleted % rows from points', points_deleted;

  -- Delete from bot_logs_statehash and count rows deleted
  WITH del AS (
    DELETE FROM bot_logs_statehash
    WHERE bot_log_id IN (SELECT id FROM bot_logs WHERE batch_start_epoch < threshold_epoch)
    RETURNING 1
  )
  SELECT COUNT(*) INTO bot_logs_statehash_deleted FROM del;
  RAISE NOTICE 'Deleted % rows from bot_logs_statehash', bot_logs_statehash_deleted;

  -- Delete from bot_logs and count rows deleted
  WITH del AS (
    DELETE FROM bot_logs
    WHERE batch_start_epoch < threshold_epoch
    RETURNING 1
  )
  SELECT COUNT(*) INTO bot_logs_deleted FROM del;
  RAISE NOTICE 'Deleted % rows from bot_logs', bot_logs_deleted;

  -- Delete from submissions and count rows deleted
  WITH del AS (
    DELETE FROM submissions
    WHERE submitted_at_date < threshold_date
    RETURNING 1
  )
  SELECT COUNT(*) INTO submissions_deleted FROM del;
  RAISE NOTICE 'Deleted % rows from submissions', submissions_deleted;

  -- Clean up statehash and count rows deleted
  WITH del AS (
    DELETE FROM statehash
    WHERE id NOT IN (SELECT DISTINCT statehash_id FROM bot_logs_statehash)
      AND id NOT IN (SELECT DISTINCT parent_statehash_id FROM bot_logs_statehash)
      AND id NOT IN (SELECT DISTINCT statehash_id FROM points)
    RETURNING 1
  )
  SELECT COUNT(*) INTO statehash_deleted FROM del;
  RAISE NOTICE 'Deleted % rows from statehash', statehash_deleted;

  -- Log the cleanup summary
  RAISE NOTICE 'Cleanup summary: % points_summary, % points, % bot_logs_statehash, % bot_logs, % submissions, % statehash',
    points_summary_deleted, points_deleted, bot_logs_statehash_deleted, bot_logs_deleted, submissions_deleted, statehash_deleted;
END;
$$ LANGUAGE plpgsql;

-- Example call to the function with 100 days
-- SELECT cleanup_old_data(100);