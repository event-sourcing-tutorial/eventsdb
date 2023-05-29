CREATE TABLE events (
  idx       bigint     not null primary key,
  inserted  timestamp  not null default now(),
  payload   jsonb      not null
);

CREATE INDEX events_idx_index on events(idx);

CREATE FUNCTION check_index() 
   RETURNS TRIGGER 
   LANGUAGE PLPGSQL
AS $$
BEGIN
   IF NEW.idx = (select coalesce(max(idx), 0) from events) + 1 THEN
     RETURN NEW;
   ELSE
     RAISE EXCEPTION 'invalid idx';
   END IF;
END;
$$;

CREATE TRIGGER check_index_trigger
  BEFORE INSERT ON events
  FOR EACH ROW EXECUTE PROCEDURE check_index();

CREATE FUNCTION emit_event()
  RETURNS TRIGGER
  LANGUAGE PLPGSQL
AS $$
DECLARE payload text;
BEGIN
  payload = concat_ws(',', NEW.idx::TEXT, replace(NEW.inserted::TEXT, ' ', 'T'), NEW.payload::TEXT);
  IF length(payload) >= 8000 THEN
    payload = concat_ws(',', NEW.idx::TEXT, replace(NEW.inserted::TEXT, ' ', 'T'));
  END IF;
  PERFORM pg_notify('event', payload);
  RETURN NEW;
END;
$$;

CREATE TRIGGER emit_event_on_insert
  AFTER INSERT ON events
  FOR EACH ROW EXECUTE PROCEDURE emit_event();

-- listen event;
-- insert into events (idx, payload) values (1, '{"msg": "hello"}'::JSONB);
-- insert into events (idx, payload) values (2, ('"' || repeat('0123456789', 800) || '"')::JSONB);
