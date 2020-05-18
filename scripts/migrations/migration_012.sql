-- Add simple word and n-gram tables for ABSA (!135)

BEGIN;

    CREATE SCHEMA absa;

    CREATE FUNCTION ensure_foreign_key(
            "table" text,
            columns text[],
            reftable text,
            refcolumns text[]
        ) RETURNS void AS
        $$
        DECLARE
            key TEXT := (
                SELECT string_agg(col, ', ')
                FROM unnest(columns) col
            );
            newkey TEXT := (
                SELECT string_agg('NEW.' || col, ', ')
                FROM unnest(columns) col
            );
            refkey TEXT := (
                SELECT string_agg(col, ', ')
                FROM unnest(refcolumns) col
            );
        BEGIN
            EXECUTE format('
                CREATE
                    OR REPLACE
                FUNCTION foreign_key_trigger()
                    RETURNS "trigger" AS
                    $BODY$ BEGIN
                        -- Disabled due to performance reasons (O(n²)) ☹
                        /*IF (SELECT (%3$s)) NOT IN (SELECT (%4$s) FROM %2$s)
                        THEN
                            RAISE EXCEPTION ''Foreign key violation: Key (%%=%%) \
                            is not present in table %%'',
                            ''(%5$s)'', (SELECT (%3$s)), ''%2$s'';
                        END IF;*/
                        RETURN NEW;
                    END; $BODY$
                    LANGUAGE ''plpgsql'';
                CREATE TRIGGER tr_before_insert_or_update
                    BEFORE INSERT OR UPDATE OF %5$s
                    ON %1$s
                    FOR EACH ROW
                    EXECUTE PROCEDURE foreign_key_trigger();
                ',
                "table",
                reftable,
                newkey,
                refkey,
                key);
        END;
        $$
        LANGUAGE 'plpgsql' VOLATILE;

    CREATE TABLE absa.stopword (
        word TEXT PRIMARY KEY
    );

    CREATE TABLE absa.post_word (
        source TEXT,
        post_id TEXT,
        word_index INT,
        word TEXT,
        PRIMARY KEY (source, post_id, word_index)
    );
    SELECT ensure_foreign_key(
        'absa.post_word', array ['source', 'post_id'],
        'absa.post', array ['source', 'post_id']
    );

    CREATE TABLE absa.post_ngram (
        source TEXT,
        post_id TEXT,
        n INT,
        word_index INT,
        ngram TEXT,
        PRIMARY KEY (source, post_id, n, word_index)
    );
    SELECT ensure_foreign_key(
        'absa.post_ngram', array ['source', 'post_id'],
        'absa.post', array ['source', 'post_id']
    );
    -- TODO: Too verbose output

COMMIT;
