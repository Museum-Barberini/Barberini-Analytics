-- TODO: Clean up and comment.
BEGIN;

    CREATE or replace FUNCTION create_progress(message text, progress text, relation text) RETURNS BOOL
    AS $$
        DECLARE count INT;
        BEGIN
            EXECUTE 'SELECT COUNT(*) FROM ' || relation INTO count;
            PERFORM exec('ALTER DATABASE ' || current_database() || ' SET progress_max.' || progress || ' = ' || count);
            RETURN TRUE;
        END;
    $$
    LANGUAGE plpgsql;

    CREATE FUNCTION query(command text) RETURNS TEXT
    AS $$
        DECLARE result TEXT;
        BEGIN
            EXECUTE command INTO result;
            RETURN result;
        END;
    $$
    LANGUAGE plpgsql;

    CREATE FUNCTION exec(command text) RETURNS VOID
    AS $$
        BEGIN
            EXECUTE command;
        END;
    $$
    LANGUAGE plpgsql;

    CREATE FUNCTION notify(message text) returns BOOL
    AS $$
    BEGIN
        RAISE NOTICE '%', message;
        RETURN TRUE;
    END;
    $$
    LANGUAGE plpgsql;

COMMIT;
