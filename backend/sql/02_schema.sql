CREATE SCHEMA IF NOT EXISTS app;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'user_role') THEN
    CREATE TYPE user_role AS ENUM ('admin', 'analyst');
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'attr_type') THEN
    CREATE TYPE attr_type AS ENUM ('string','number','boolean','datetime','object','array');
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'rule_action') THEN
    CREATE TYPE rule_action AS ENUM ('alert','ignore','block','force_review');
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'severity_level') THEN
    CREATE TYPE severity_level AS ENUM ('info','warn','high');
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'decision_type') THEN
    CREATE TYPE decision_type AS ENUM ('legit','alert','invalid');
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS app.users (
  user_id         BIGSERIAL PRIMARY KEY,
  username        CITEXT NOT NULL UNIQUE,
  password_hash   TEXT   NOT NULL,
  role            user_role NOT NULL DEFAULT 'analyst',
  is_active       BOOLEAN NOT NULL DEFAULT TRUE,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_login_at   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_users_role ON app.users(role);

CREATE TABLE IF NOT EXISTS app.entities (
  entity_id     BIGSERIAL PRIMARY KEY,
  entity_key    TEXT NOT NULL UNIQUE,
  name          TEXT NOT NULL,
  description   TEXT,
  is_active     BOOLEAN NOT NULL DEFAULT TRUE,
  created_by    BIGINT REFERENCES app.users(user_id),
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS app.entity_attributes (
  attr_id       BIGSERIAL PRIMARY KEY,
  entity_id     BIGINT NOT NULL REFERENCES app.entities(entity_id) ON DELETE CASCADE,
  attr_key      TEXT NOT NULL,
  name          TEXT NOT NULL,
  type          attr_type NOT NULL,
  is_required   BOOLEAN NOT NULL DEFAULT FALSE,
  default_value JSONB,
  description   TEXT,
  UNIQUE(entity_id, attr_key)
);

CREATE INDEX IF NOT EXISTS idx_entity_attributes_entity ON app.entity_attributes(entity_id);

CREATE TABLE IF NOT EXISTS app.rules (
  rule_id       BIGSERIAL PRIMARY KEY,
  entity_id     BIGINT NOT NULL REFERENCES app.entities(entity_id) ON DELETE CASCADE,
  name          TEXT NOT NULL,
  description   TEXT,
  is_active     BOOLEAN NOT NULL DEFAULT TRUE,
  priority      INT NOT NULL DEFAULT 100,
  severity      severity_level NOT NULL DEFAULT 'warn',
  action        rule_action NOT NULL DEFAULT 'alert',
  created_by    BIGINT REFERENCES app.users(user_id),
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS app.rule_versions (
  rule_version_id BIGSERIAL PRIMARY KEY,
  rule_id         BIGINT NOT NULL REFERENCES app.rules(rule_id) ON DELETE CASCADE,
  version_no      INT NOT NULL,
  expr_type       TEXT NOT NULL DEFAULT 'jsonlogic',
  expr            JSONB NOT NULL,
  created_by      BIGINT REFERENCES app.users(user_id),
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(rule_id, version_no)
);

CREATE TABLE IF NOT EXISTS app.rule_active_version (
  rule_id         BIGINT PRIMARY KEY REFERENCES app.rules(rule_id) ON DELETE CASCADE,
  rule_version_id BIGINT NOT NULL REFERENCES app.rule_versions(rule_version_id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_rules_entity_active ON app.rules(entity_id, is_active);

CREATE TABLE IF NOT EXISTS app.events (
  event_pk      BIGSERIAL PRIMARY KEY,
  event_id      UUID NOT NULL UNIQUE,
  entity_id     BIGINT NOT NULL REFERENCES app.entities(entity_id),
  occurred_at   TIMESTAMPTZ NOT NULL,
  payload       JSONB NOT NULL,
  meta          JSONB NOT NULL DEFAULT '{}'::jsonb,
  received_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_events_entity_time ON app.events(entity_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_payload_gin ON app.events USING GIN (payload);

CREATE TABLE IF NOT EXISTS app.classifications (
  class_pk       BIGSERIAL PRIMARY KEY,
  event_pk       BIGINT NOT NULL REFERENCES app.events(event_pk) ON DELETE CASCADE,
  decision       decision_type NOT NULL,
  risk           NUMERIC(5,3) NOT NULL DEFAULT 0,
  matched_rules  JSONB NOT NULL DEFAULT '[]'::jsonb,
  reasons        JSONB NOT NULL DEFAULT '[]'::jsonb,
  classified_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_classifications_decision_time
  ON app.classifications(decision, classified_at DESC);
