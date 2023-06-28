-----------------------------------------------------------------------------
-- COMMANDS

CREATE TYPE command_status AS ENUM ('issued', 'finalized');

CREATE TYPE final_status AS ENUM ('succeeded', 'failed', 'aborted');

CREATE TABLE issued_commands (
  command_id    uuid           not null primary key,
  command_type  text           not null,
  command_data  jsonb          not null,
  inserted      timestamp      not null default now()
);

CREATE TABLE finalized_commands (
  command_id uuid         not null primary key,
  status     final_status not null,
  foreign key (command_id) references issued_commands (command_id)
);

CREATE TABLE command_stream (
  idx         bigint         not null primary key,
  command_id  uuid           not null,
  status      command_status not null,
  updated     timestamp      not null default now(),
  foreign key (command_id) references issued_commands (command_id)
);

CREATE FUNCTION stream_command()
  RETURNS TRIGGER
  LANGUAGE PLPGSQL
AS $$
BEGIN
  INSERT INTO command_stream (idx, command_id, status)
    VALUES (
      (select coalesce(max(idx), 0) + 1 from command_stream),
      NEW.command_id,
      TG_ARGV[0]::command_status
    );
  RETURN NEW;
END;
$$;

CREATE TRIGGER on_command_issued
  AFTER INSERT ON issued_commands
  FOR EACH ROW EXECUTE PROCEDURE stream_command('issued');

CREATE TRIGGER on_command_finalized
  AFTER INSERT ON finalized_commands
  FOR EACH ROW EXECUTE PROCEDURE stream_command('finalized');

CREATE FUNCTION emit_command()
  RETURNS TRIGGER
  LANGUAGE PLPGSQL
AS $$
BEGIN
  PERFORM pg_notify('command', concat_ws(',', NEW.idx, NEW.command_id, NEW.status));
  RETURN NEW;
END;
$$;

CREATE TRIGGER on_command_updated
  AFTER INSERT ON command_stream
  FOR EACH ROW EXECUTE PROCEDURE emit_command();

-----------------------------------------------------------------------------
-- EVENTS

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

-----------------------------------------------------------------------------
-- EXAMPLES

-- listen event;
-- insert into events (idx, payload) values (1, '{"msg": "hello"}'::JSONB);
-- insert into events (idx, payload) values (2, ('"' || repeat('0123456789', 800) || '"')::JSONB);

-- listen command;
--
-- insert into issued_commands (command_id, command_type, command_data)
-- values ('D86C4652-267F-4A29-98C4-F284541AAE0D', 'foobar', '{"foo": 12}');
--
-- insert into finalized_commands (command_id, status)
-- values ('D86C4652-267F-4A29-98C4-F284541AAE0D', 'aborted');
--
-- select * from issued_commands;
-- select * from finalized_commands;
-- select * from command_stream;
--
