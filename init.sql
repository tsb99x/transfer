-- Account table with unique service account

CREATE TABLE account (
    id UUID PRIMARY KEY,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

INSERT INTO account (id)
VALUES ('00000000-0000-0000-0000-000000000000');

-- Main transfer table with constraints, indexes, checks and triggers

CREATE TABLE transfer (
    source UUID NOT NULL REFERENCES account(id),
    index INTEGER NOT NULL,
    destination UUID NOT NULL REFERENCES account(id),
    amount DECIMAL NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (source, index),
    CONSTRAINT transfer_positive_amount CHECK (amount > 0),
    CONSTRAINT transfer_not_same_account CHECK (source != destination)
);

CREATE INDEX transfer_source ON transfer(source);
CREATE INDEX transfer_destination ON transfer(destination);
CREATE INDEX transfer_created_at ON transfer(created_at);

-- Tricky function to remove SQL aggregation mess from service application

CREATE FUNCTION account_metadata (IN UUID, IN TIMESTAMPTZ)
RETURNS TABLE (balance NUMERIC, next_transfer_index INTEGER)
AS $BODY$
    -- If some values not found after left join (NULL), replace them with 0
    SELECT
       COALESCE(incoming_sum, 0) - COALESCE(outgoing_sum, 0) AS balance,
       COALESCE(last_index + 1, 0) AS next_transfer_index
    -- Enforce account existence in account table
    FROM (SELECT
              id
          FROM account
          WHERE id = $1) a
    -- Join with sum of all incoming transfers until specified timestamp
    LEFT JOIN (SELECT
                   destination AS id,
                   SUM(amount) AS incoming_sum
               FROM transfer
               WHERE destination = $1
               AND created_at <= $2
               GROUP BY destination) i ON i.id = a.id
    -- Join with sum of all outgoing transfers and last transfer index until specified timestamp
    LEFT JOIN (SELECT
                   source AS id,
                   SUM(amount) AS outgoing_sum,
                   MAX(index) AS last_index
               FROM transfer
               WHERE source = $1
               AND created_at <= $2
               GROUP BY source) o ON o.id = a.id;
$BODY$
LANGUAGE SQL;

-- Trigger to check source balance exactly BEFORE insertion of new transfer

CREATE FUNCTION check_source_balance()
RETURNS TRIGGER
AS $BODY$
    DECLARE
        source_balance DECIMAL;
    BEGIN
        -- Just ignore service account, because it is a special account
        IF NEW.source = '00000000-0000-0000-0000-000000000000' THEN
            RETURN NEW;
        END IF;

        SELECT balance
        INTO source_balance
        FROM account_metadata(NEW.source, NOW());

        IF source_balance < NEW.amount THEN
            RAISE EXCEPTION '% balance should be bigger than %', new.source, new.amount;
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
RETURNS TRIGGER
AS $BODY$
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