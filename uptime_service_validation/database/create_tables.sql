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
	score_percent NUMERIC(10,2),
	discord_id TEXT,
	email_id TEXT,
	application_status BOOLEAN
);

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
	score_percent NUMERIC(10,2),
	CONSTRAINT fk_nodes
		FOREIGN KEY(node_id) 
		REFERENCES nodes(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_sh_node_score_at ON score_history USING btree (node_id, score_at);