-- This is the PostgreSQL compatible schema for Redka

-- ┌───────────────┐
-- │ Keys          │
-- └───────────────┘
-- Types:
-- 1 - string
-- 2 - list
-- 3 - set
-- 4 - hash
-- 5 - zset (sorted set)
CREATE TABLE IF NOT EXISTS
rkey (
    id       SERIAL PRIMARY KEY,
    key      TEXT NOT NULL,
    type     INTEGER NOT NULL,
    version  INTEGER NOT NULL,
    etime    BIGINT,
    mtime    BIGINT NOT NULL,
    len      INTEGER
);

CREATE UNIQUE INDEX IF NOT EXISTS
rkey_key_idx ON rkey (key);

CREATE INDEX IF NOT EXISTS
rkey_etime_idx ON rkey (etime)
WHERE etime IS NOT NULL;

CREATE OR REPLACE VIEW
vkey AS
SELECT
    id AS kid, key, type, len,
    to_timestamp(etime::double precision/1000) AS etime,
    to_timestamp(mtime::double precision/1000) AS mtime
FROM rkey
WHERE rkey.etime IS NULL OR rkey.etime > (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT;

-- ┌───────────────┐
-- │ Strings       │
-- └───────────────┘
CREATE TABLE IF NOT EXISTS
rstring (
    kid    INTEGER NOT NULL,
    value  BYTEA NOT NULL,

    FOREIGN KEY (kid) REFERENCES rkey (id)
    ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS
rstring_pk_idx ON rstring (kid);

CREATE OR REPLACE VIEW
vstring AS
SELECT
    rkey.id AS kid, rkey.key, rstring.value,
    to_timestamp(etime::double precision/1000) AS etime,
    to_timestamp(mtime::double precision/1000) AS mtime
FROM rstring JOIN rkey ON rstring.kid = rkey.id AND rkey.type = 1
WHERE rkey.etime IS NULL OR rkey.etime > (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT;

-- ┌───────────────┐
-- │ Lists         │
-- └───────────────┘
CREATE TABLE IF NOT EXISTS
rlist (
    kid    INTEGER NOT NULL,
    pos    REAL NOT NULL,
    elem   BYTEA NOT NULL,

    FOREIGN KEY (kid) REFERENCES rkey (id)
    ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS
rlist_pk_idx ON rlist (kid, pos);

-- Create triggers for list updates
CREATE OR REPLACE FUNCTION update_rkey_on_rlist_update() RETURNS TRIGGER AS $$
BEGIN
    UPDATE rkey SET
        version = version + 1,
        mtime = (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT
    WHERE id = OLD.kid;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER rlist_on_update
BEFORE UPDATE ON rlist
FOR EACH ROW
EXECUTE FUNCTION update_rkey_on_rlist_update();

CREATE OR REPLACE FUNCTION update_rkey_on_rlist_delete() RETURNS TRIGGER AS $$
BEGIN
    UPDATE rkey SET
        version = version + 1,
        mtime = (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT,
        len = len - 1
    WHERE id = OLD.kid;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER rlist_on_delete
BEFORE DELETE ON rlist
FOR EACH ROW
EXECUTE FUNCTION update_rkey_on_rlist_delete();

CREATE OR REPLACE VIEW
vlist AS
SELECT
    rkey.id AS kid, rkey.key,
    ROW_NUMBER() OVER w AS idx, rlist.elem,
    to_timestamp(etime::double precision/1000) AS etime,
    to_timestamp(mtime::double precision/1000) AS mtime
FROM rlist JOIN rkey ON rlist.kid = rkey.id AND rkey.type = 2
WHERE rkey.etime IS NULL OR rkey.etime > (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT
WINDOW w AS (PARTITION BY kid ORDER BY pos);

-- ┌───────────────┐
-- │ Sets          │
-- └───────────────┘
CREATE TABLE IF NOT EXISTS
rset (
    kid    INTEGER NOT NULL,
    elem   BYTEA NOT NULL,

    FOREIGN KEY (kid) REFERENCES rkey (id)
    ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS
rset_pk_idx ON rset (kid, elem);

-- Create trigger for set inserts
CREATE OR REPLACE FUNCTION update_rkey_on_rset_insert() RETURNS TRIGGER AS $$
BEGIN
    UPDATE rkey
    SET len = len + 1
    WHERE id = NEW.kid;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER rset_on_insert
AFTER INSERT ON rset
FOR EACH ROW
EXECUTE FUNCTION update_rkey_on_rset_insert();

CREATE OR REPLACE VIEW
vset AS
SELECT
    rkey.id AS kid, rkey.key, rset.elem,
    to_timestamp(etime::double precision/1000) AS etime,
    to_timestamp(mtime::double precision/1000) AS mtime
FROM rset JOIN rkey ON rset.kid = rkey.id AND rkey.type = 3
WHERE rkey.etime IS NULL OR rkey.etime > (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT;

-- ┌───────────────┐
-- │ Hashes        │
-- └───────────────┘
CREATE TABLE IF NOT EXISTS
rhash (
    kid   INTEGER NOT NULL,
    field TEXT NOT NULL,
    value BYTEA NOT NULL,

    FOREIGN KEY (kid) REFERENCES rkey (id)
    ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS
rhash_pk_idx ON rhash (kid, field);

-- Create trigger for hash inserts
CREATE OR REPLACE FUNCTION update_rkey_on_rhash_insert() RETURNS TRIGGER AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM rhash
        WHERE kid = NEW.kid AND field = NEW.field
    ) THEN
        UPDATE rkey
        SET len = len + 1
        WHERE id = NEW.kid;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER rhash_on_insert
BEFORE INSERT ON rhash
FOR EACH ROW
EXECUTE FUNCTION update_rkey_on_rhash_insert();

CREATE OR REPLACE VIEW
vhash AS
SELECT
    rkey.id AS kid, rkey.key, rhash.field, rhash.value,
    to_timestamp(etime::double precision/1000) AS etime,
    to_timestamp(mtime::double precision/1000) AS mtime
FROM rhash JOIN rkey ON rhash.kid = rkey.id AND rkey.type = 4
WHERE rkey.etime IS NULL OR rkey.etime > (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT;

-- ┌───────────────┐
-- │ Sorted sets   │
-- └───────────────┘
CREATE TABLE IF NOT EXISTS
rzset (
    kid    INTEGER NOT NULL,
    elem   BYTEA NOT NULL,
    score  REAL NOT NULL,

    FOREIGN KEY (kid) REFERENCES rkey (id)
    ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS
rzset_pk_idx ON rzset (kid, elem);

CREATE INDEX IF NOT EXISTS
rzset_score_idx ON rzset (kid, score, elem);

-- Create trigger for zset inserts
CREATE OR REPLACE FUNCTION update_rkey_on_rzset_insert() RETURNS TRIGGER AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM rzset
        WHERE kid = NEW.kid AND elem = NEW.elem
    ) THEN
        UPDATE rkey
        SET len = len + 1
        WHERE id = NEW.kid;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER rzset_on_insert
BEFORE INSERT ON rzset
FOR EACH ROW
EXECUTE FUNCTION update_rkey_on_rzset_insert();

CREATE OR REPLACE VIEW
vzset AS
SELECT
    rkey.id AS kid, rkey.key, rzset.elem, rzset.score,
    to_timestamp(etime::double precision/1000) AS etime,
    to_timestamp(mtime::double precision/1000) AS mtime
FROM rzset JOIN rkey ON rzset.kid = rkey.id AND rkey.type = 5
WHERE rkey.etime IS NULL OR rkey.etime > (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT; 