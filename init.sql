-- Account table with unique service account

CREATE TABLE account (
    id UUID PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO account(id)
VALUES ('00000000-0000-0000-0000-000000000000');

-- Main transfer table with constraints, indexes, checks and triggers

CREATE TABLE transfer (
    source UUID NOT NULL REFERENCES account(id),
    index INTEGER NOT NULL,
    destination UUID NOT NULL REFERENCES account(id),
    amount DECIMAL NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (source, index),
    CONSTRAINT transfer_positive_amount CHECK (amount > 0),
    CONSTRAINT transfer_not_same_account CHECK (source != destination)
);

CREATE INDEX transfer_source ON transfer(source);
CREATE INDEX transfer_destination ON transfer(destination);
CREATE INDEX transfer_created_at ON transfer(created_at);

-- Tricky function to remove SQL aggregation mess from service application

CREATE FUNCTION account_metadata(UUID[])
RETURNS TABLE (id UUID, balance NUMERIC, next_transfer_index INTEGER) AS $BODY$
    BEGIN
        RETURN QUERY
        -- If some values not found after left join (NULL), replace them with 0
        SELECT
           a.id,
           COALESCE(incoming_sum, 0) - COALESCE(outgoing_sum, 0) AS balance,
           COALESCE(last_index + 1, 0) AS next_transfer_index
        -- Enforce accounts existence in account table
        FROM (SELECT
                  a.id
              FROM account a
              WHERE a.id = ANY($1)) a
        -- Join with sum of all incoming transfers until specified timestamp
        LEFT JOIN (SELECT
                       destination AS id,
                       SUM(amount) AS incoming_sum
                   FROM transfer
                   WHERE destination = ANY($1)
                   GROUP BY destination) i ON i.id = a.id
        -- Join with sum of all outgoing transfers and last transfer index until specified timestamp
        LEFT JOIN (SELECT
                       source AS id,
                       SUM(amount) AS outgoing_sum,
                       MAX(index) AS last_index
                   FROM transfer
                   WHERE source = ANY($1)
                   GROUP BY source) o ON o.id = a.id;
    END;
$BODY$
LANGUAGE PLPGSQL;

-- Trigger to check source balance exactly BEFORE insertion of new transfer

CREATE FUNCTION check_source_balance()
RETURNS TRIGGER AS $BODY$
    DECLARE
        source_balance DECIMAL;
    BEGIN
        -- Just ignore service account, because it is a special account
        IF NEW.source = '00000000-0000-0000-0000-000000000000' THEN
            RETURN NEW;
        END IF;

        SELECT balance
        INTO source_balance
        FROM account_metadata(ARRAY[NEW.source]::uuid[]);

        IF source_balance < NEW.amount THEN
            RAISE EXCEPTION '% balance should be bigger than %', NEW.source, NEW.amount;
        END IF;

        RETURN NEW;
    END;
$BODY$
LANGUAGE PLPGSQL;

CREATE TRIGGER transfer_check_source_balance
BEFORE INSERT ON transfer
FOR EACH ROW EXECUTE PROCEDURE check_source_balance();

-- Trigger to check indexing (order) of transfer BEFORE insertion of new one

CREATE FUNCTION check_indexing()
RETURNS TRIGGER AS $BODY$
    DECLARE
        expected_index INTEGER;
    BEGIN
        SELECT COALESCE(MAX(index) + 1, 0)
        INTO expected_index
        FROM transfer
        WHERE source = NEW.source;

        IF expected_index != NEW.index THEN
            RAISE EXCEPTION 'next index for transfer from % should be %', NEW.source, expected_index;
        END IF;

        RETURN NEW;
    END;
$BODY$
LANGUAGE PLPGSQL;

CREATE TRIGGER transfer_check_indexing
BEFORE INSERT ON transfer
FOR EACH ROW EXECUTE PROCEDURE check_indexing();

-- Naive account initialization to get rid of unnecessary round-trips to DB

CREATE FUNCTION init_account(UUID, NUMERIC)
RETURNS VOID AS $BODY$
    DECLARE
        next_index INTEGER;
    BEGIN
        INSERT INTO account(id)
        VALUES ($1);

        IF $2 != 0 THEN
            SELECT COALESCE(MAX(index) + 1, 0)
            INTO next_index
            FROM transfer
            WHERE source = '00000000-0000-0000-0000-000000000000';

            INSERT INTO transfer(source, index, destination, amount)
            VALUES ('00000000-0000-0000-0000-000000000000', next_index, $1, $2);
        END IF;
    END;
$BODY$
LANGUAGE PLPGSQL;

-- Testing view to see all accounts and their metadata, not used in service directly

CREATE VIEW combined_metadata AS
    SELECT
    	a.id,
        COALESCE(incoming_sum, 0) - COALESCE(outgoing_sum, 0) AS balance,
        COALESCE(last_index + 1, 0) AS next_transfer_index
    -- Get all account ids
    FROM (SELECT
              id
          FROM account) a
    -- Join with sum of all incoming transfers
    LEFT JOIN (SELECT
                   destination AS id,
                   SUM(amount) AS incoming_sum
               FROM transfer
               GROUP BY destination) i ON i.id = a.id
    -- Join with sum of all outgoing transfers and last transfer index
    LEFT JOIN (SELECT
                   source AS id,
                   SUM(amount) AS outgoing_sum,
                   MAX(index) AS last_index
               FROM transfer
               GROUP BY source) o ON o.id = a.id;
