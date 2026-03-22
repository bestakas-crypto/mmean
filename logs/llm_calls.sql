BEGIN TRANSACTION;
CREATE TABLE IF NOT EXISTS "llm_calls" (
	"id"	INTEGER,
	"ts"	TEXT NOT NULL,
	"layer"	TEXT NOT NULL,
	"provider"	TEXT NOT NULL,
	"prompt_json"	TEXT,
	"response_json"	TEXT NOT NULL,
	"latency_ms"	INTEGER,
	"applied"	INTEGER DEFAULT 0,
	"validation_ok"	INTEGER DEFAULT 1,
	"session_date"	TEXT,
	"baseline_hash"	TEXT,
	"apply_mode"	TEXT,
	"error_type"	TEXT,
	"fallback_depth"	INTEGER DEFAULT 0,
	"attempts_json"	TEXT,
	"expires_at"	TEXT,
	"opportunity_score"	REAL,
	"direction"	TEXT,
	"prompt_name"	TEXT,
	"prompt_hash"	TEXT,
	"prompt_source"	TEXT,
	PRIMARY KEY("id" AUTOINCREMENT)
);
CREATE INDEX IF NOT EXISTS "idx_llm_calls_layer_score" ON "llm_calls" (
	"layer",
	"opportunity_score"
);
CREATE INDEX IF NOT EXISTS "idx_llm_calls_prompt_hash" ON "llm_calls" (
	"prompt_hash"
);
CREATE INDEX IF NOT EXISTS "idx_llm_calls_session_layer" ON "llm_calls" (
	"session_date",
	"layer"
);
COMMIT;
