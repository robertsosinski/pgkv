--------------------
-- Start Table Setup
--------------------

-- create keyval schema for all key/value types (strings, numbers, hashes, arrays)
create schema keyval;

-- create the key/value table for strings
create table keyval.strings (
  key varchar primary key,
  value text,
  created_at timestamp,
  updated_at timestamp
);

-- create the key/value table for numbers
create table keyval.numbers (
  key varchar primary key,
  value int not null default 0,
  created_at timestamp,
  updated_at timestamp
);

-- create the key/value table for lists
create table keyval.lists (
  key varchar primary key,
  value text[],
  created_at timestamp,
  updated_at timestamp
);

-- create the key/value table for hashes
create extension hstore;

create table keyval.hashes (
  key varchar primary key,
  value hstore,
  created_at timestamp,
  updated_at timestamp
);

-------------------------
-- Start String Functions
-------------------------

-- KVAPPEND: Appends the value to the end of the string if the key has already been set.
--           If the key does not exist, sets the key to hold the string value instead.
--           The length of the current string value is always returned.
--  ARG1: keyname varchar
--  ARG2: valuestring text
--  RTRN: integer
--
-- EXAMPLE 1:
--  select * from kvappend('abc', 'hello');
--   kvappend
--  ----------
--          5
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvappend('abc', 'world');
--   kvappend
--  ----------
--         10
--  (1 row)
create or replace function kvappend(keyname varchar, valuestring text) returns int as $$
declare
  result int;
begin
  update keyval.strings set value = (value || valuestring), updated_at = now() where key = keyname returning char_length(value) into result;
  if not found then
    insert into keyval.strings (key, value, created_at, updated_at) values (keyname, valuestring, now(), now());
    result := char_length(valuestring);
  end if;
  return result;
end;
$$ language 'plpgsql';

-- KVDEL: Deletes the string value held by the key and returns TRUE.
--        If the key does not exist, FALSE is returned instead.
--  ARG1: keyname varchar
--  RTRN: boolean
--
-- EXAMPLE 1:
--  select * from kvdel('abc');
--   kvdel
--  -------
--   t
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvdel('nonexistent');
--
--   kvdel
--  -------
--   f
--  (1 row)
create or replace function kvdel(keyname varchar) returns boolean as $$
declare
  result boolean := false;
begin
  delete from keyval.strings where key = keyname;
  if found then
    result := true;
  end if;
  return result;
end;
$$ language 'plpgsql';

-- KVDELE: Deletes the string value held by the key.
--         If the key does not exist, an error is raised instead.
--  ARG1: keyname varchar
--  RTRN: void
--
-- EXAMPLE 1:
--  select * from kvdele('abc');
--   kvdele
--  --------
--
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvdele('nonexistent');
--  ERROR:  The keyname provided does not exists!
create or replace function kvdele(keyname varchar) returns void as $$
begin
  if not kvdel(keyname) then
    raise exception 'The keyname provided does not exist!';
  end if;
end;
$$ language 'plpgsql';

-- KVGET: Returns the string value of the key.
--        If the key does not exist, NULL is returned instead.
--  ARG1: keyname varchar
--  RTRN: text
--
-- EXAMPLE 1:
--  select * from kvget('abc');
--      kvget
--  -------------
--   hello world
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvget('nonexistent');
--   kvget
--  --------
--   <NULL>
--  (1 row)
create or replace function kvget(keyname varchar) returns text as $$
declare
  result text;
begin
  select value from keyval.strings where key = keyname into result;
  return result;
end;
$$ language 'plpgsql';

-- KVGETSET: Sets the key to hold a string value and returns the old value stored.
--           If no value was previously stored, NULL is returned instead.
--  ARG1: keyname varchar
--  ARG2: valuestring text
--  RTRN: text
--
-- EXAMPLE 1:
--  select * from kvgetset('abc', 'hello world');
--   kvgetset
--  ----------
--   <NULL>
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvgetset('abc', 'howdy partner');
--     kvgetset
--  -------------
--   hello world
--  (1 row)
create or replace function kvgetset(keyname varchar, valuestring text) returns text as $$
declare
  result text;
begin
  select value from keyval.strings where key = keyname into result;
  if found then
    update keyval.strings set value = valuestring, updated_at = now() where key = keyname;
  else
    insert into keyval.strings (key, value, created_at, updated_at) values (keyname, valuestring, now(), now());
  end if;
  return result;
end;
$$ language 'plpgsql';

-- KVLEN: Returns the length of the string value of the key.
--        If no value was previously stored, NULL is returned instead.
--  ARG1: keyname varchar
--  RTRN: int
--
-- EXAMPLE 1:
--  select * from kvlen('abc');
--   kvlen
--  -------
--      11
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvlen('nonexistent');
--   kvlen
--  --------
--   <NULL>
--  (1 row)
create or replace function kvlen(keyname varchar) returns int as $$
declare
  result int;
begin
  select char_length(value) from keyval.strings where key = keyname into result;
  if not found then
    result := null;
  end if;
  return result;
end;
$$ language 'plpgsql';

-- KVMGET: Returns the key and string values of all specified keys.
--         For any keys specified that do not have a string value, NULL is returned instead.
--  ARG1: keynames varchar[]
--  RTRN: table(key varchar, value text)
--
-- EXAMPLE 1:
--  select * from kvmget(array['abc', 'nonexistent', 'def', 'nonexistent']);
--       key     |     value
--  -------------+---------------
--   abc         | hello world
--   nonexistent | <NULL>
--   def         | howdy partner
--   nonexistent | <NULL>
--  (4 rows)
create or replace function kvmget(keynames varchar[]) returns table(key varchar, value text) as $$
begin
  return query with
    keys as (select unnest(keynames))
    select keys.unnest, strings.value from keys
    left outer join keyval.strings on strings.key = keys.unnest;
end;
$$ language 'plpgsql';

-- KVMSET: Sets all keys to their respective string values.
--         For any keys that already hold a value, they will be overwritten instead.
--         If more keys then values are given, an error will be raised.
--         If more values then keys are given, an error will be raised.
--  ARG1: keynames varchar[]
--  ARG2: valuestrings text[]
--  RTRN: void
--
-- EXAMPLE 1:
--  select * from kvmset(array['a', 'b', 'c'], array['apple', 'banana', 'cherry']);
--   kvmset
--  --------
--
--  (1 row)
create or replace function kvmset(keynames varchar[], valuestrings text[]) returns void as $$
declare
  keyname varchar;
  i int := 1;
begin
  if array_length(keynames, 1) = array_length(valuestrings, 1) then
    foreach keyname in array keynames loop
      update keyval.strings set value = valuestrings[i], updated_at = now() where key = keyname;
      if not found then
        insert into keyval.strings (key, value, created_at, updated_at) values (keyname, valuestrings[i], now(), now());
      end if;
      i := i + 1;
    end loop;
  else
    raise exception 'The size of the "keynames" and "valuestrings" arguments must match!';
  end if;
end;
$$ language 'plpgsql';

-- KVMSETNX: Sets all keys to their respective string values, if all of the keys specified have not been set already and returns TRUE.
--           If any of the specified keys have already been set to a string value, this function will do nothing and return FALSE instead.
--           If more keys then values are given, an error will be raised.
--           If more values then keys are given, an error will be raised.
--  ARG1: keynames varchar[]
--  ARG2: valuestrings text[]
--  RTRN: boolean
--
-- EXAMPLE 1:
--  select * from kvmsetnx(array['a', 'b', 'c'], array['apple', 'banana', 'cherry']);
--   kvmsetnx
--  ----------
--   t
--  (1 row)
-- EXAMPLE 1:
--  select * from kvmsetnx(array['a', 'd', 'e'], array['apricot', 'date', 'eggplant']);
--   kvmsetnx
--  ----------
--   f
--  (1 row)
create or replace function kvmsetnx(keynames varchar[], valuestrings text[]) returns boolean as $$
declare
  result boolean := true;
  keyname varchar;
  i int := 1;
begin
  if array_length(keynames, 1) = array_length(valuestrings, 1) then
    perform key from keyval.strings where key = any(keynames) limit 1;
    if found then
      result := false;
    else
      foreach keyname in array keynames loop
        update keyval.strings set value = valuestrings[i], updated_at = now() where key = keyname;
        if not found then
          insert into keyval.strings (key, value, created_at, updated_at) values (keyname, valuestrings[i], now(), now());
        end if;
        i := i + 1;
      end loop;
    end if;
  else
    raise exception 'The size of the "keynames" and "valuestrings" arguments must match!';
  end if;
  return result;
end;
$$ language 'plpgsql';

-- KVMSETNXE: Sets all keys to their respective string values, if all of the keys specified have not been set already.
--            If any of the specified keys have already been set to a string value, this function will do nothing and raise an error.
--            If more keys then values are given, an error will be raised.
--            If more values then keys are given, an error will be raised.
--  ARG1: keynames varchar[]
--  ARG2: valuestrings text[]
--  RTRN: void
--
-- EXAMPLE 1:
--  select * from kvmsetnx(array['a', 'b', 'c'], array['apple', 'banana', 'cherry']);
--   kvmsetnxe
--  -----------
--
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvmsetnx(array['a', 'd', 'e'], array['apricot', 'date', 'eggplant']);
--  ERROR:  One or more of the keynames provided already exist!
create or replace function kvmsetnxe(keynames varchar[], valuestrings text[]) returns void as $$
begin
  if not kvmsetnx(keynames, valuestrings) then
    raise exception 'One or more of the keynames provided already exist!';
  end if;
end;
$$ language 'plpgsql';

-- KVSET: Sets the key to hold a string value.
--        If key already holds a value, it is overwritten instead.
--  ARG1: keyname varchar
--  ARG2: valuestring text
--  RTRN: void
--
-- EXAMPLE 1:
--  select * from kvset('abc', 'hello world');
--   kvset
--  -------
--
--  (1 row)
create or replace function kvset(keyname varchar, valuestring text) returns void as $$
begin
  update keyval.strings set value = valuestring, updated_at = now() where key = keyname;
  if not found then
    insert into keyval.strings (key, value, created_at, updated_at) values (keyname, valuestring, now(), now());
  end if;
end;
$$ language 'plpgsql';

-- KVSETNX: Sets the key to hold a string value, if the key does not exist, and returns TRUE.
--          If the key already has a value set, no change is made and FALSE is returned instead.
--  ARG1: keyname varchar
--  ARG2: valuestring text
--  RTRN: boolean
--
-- EXAMPLE 1:
--  select * from kvsetnx('abc', 'hello world');
--   kvsetnx
--  ---------
--   t
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvsetnx('abc', 'howdy partner');
--   kvsetnx
--  ---------
--   f
--  (1 row)
create or replace function kvsetnx(keyname varchar, valuestring text) returns boolean as $$
declare
  result boolean := true;
begin
  begin
    insert into keyval.strings (key, value, created_at, updated_at) values (keyname, valuestring, now(), now());
  exception when unique_violation then
    result := false;
  end;
  return result;
end;
$$ language 'plpgsql';

-- KVSETNXE: Sets the key to hold a string value, if the key does not exist.
--           If the key already has a value set, no change is made and an raise an error.
--  ARG1: keyname varchar
--  ARG2: valuestring text
--  RTRN: boolean
--
-- EXAMPLE 1:
--  select * from kvsetnx('abc', 'hello world');
--   kvsetnxe
--  ----------
--
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvsetnxe('abc', 'howdy partner');
--  ERROR:  The keyname provided already exists!
create or replace function kvsetnxe(keyname varchar, valuestring text) returns void as $$
begin
  if not kvsetnx(keyname, valuestring) then
    raise exception 'The keyname provided already exists!';
  end if;
end;
$$ language 'plpgsql';

-------------------------
-- Start Number Functions
-------------------------

-- KVNDEL: Deletes the number value held by the key and returns TRUE.
--         If the key does not exist, FALSE is returned instead.
--  ARG1: keyname varchar
--  RTRN: void
--
-- EXAMPLE 1:
--  select * from kvndel('abc');
--   kvndel
--  --------
--   t
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvndel('nonexistent');
--
--   kvndel
--  --------
--   f
--  (1 row)
create or replace function kvndel(keyname varchar) returns boolean as $$
declare
  result boolean := false;
begin
  delete from keyval.numbers where key = keyname;
  if found then
    result := true;
  end if;
  return result;
end;
$$ language 'plpgsql';

-- KVNDELE: Deletes the number value held by the key.
--          If the key does not exist, an error is raised instead.
--  ARG1: keyname varchar
--  RTRN: void
--
-- EXAMPLE 1:
--  select * from kvdele('abc');
--   kvndele
--  ---------
--
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvndele('nonexistent');
--  ERROR:  The keyname provided does not exists!
create or replace function kvndele(keyname varchar) returns void as $$
begin
  if not kvndel(keyname) then
    raise exception 'The keyname provided does not exist!';
  end if;
end;
$$ language 'plpgsql';

-- KVNGET: Returns the number value of the key.
--         If the key does not exist, NULL is returned instead.
--  ARG1: keyname varchar
--  RTRN: text
--
-- EXAMPLE 1:
--  select * from kvnget('abc');
--   kvnget
--  --------
--        5
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvnget('nonexistent');
--   kvnget
--  --------
--   <NULL>
--  (1 row)
create or replace function kvnget(keyname varchar) returns int as $$
declare
  result int;
begin
  select value from keyval.numbers where key = keyname into result;
  return result;
end;
$$ language 'plpgsql';

-- KVNGETSET: Sets the key to hold a number value and returns the old value stored.
--            If no value was previously stored, NULL is returned instead.
--  ARG1: keyname varchar
--  ARG2: valuenumber int
--  RTRN: int
--
-- EXAMPLE 1:
--  select * from kvgetset('abc', 3);
--   kvngetset
--  -----------
--   <NULL>
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvgetset('abc', 7);
--   kvngetset
--  -----------
--   3
--  (1 row)
create or replace function kvngetset(keyname varchar, valuenumber int) returns int as $$
declare
  result int;
begin
  select value from keyval.numbers where key = keyname into result;
  if found then
    update keyval.numbers set value = valuenumber, updated_at = now() where key = keyname;
  else
    insert into keyval.numbers (key, value, created_at, updated_at) values (keyname, valuenumber, now(), now());
  end if;
  return result;
end;
$$ language 'plpgsql';

-- KVNINCRBY: Increments the value stored at the key by the number given.
--            If the key does not have a value, the number given becomes the new value instead.
--  ARG1: keyname varchar
--  ARG2: valuenumber int
--  RTRN: int
--
-- EXAMPLE 1:
--  select * from kvnincrby('abc', 5);
--   kvnincrby
--  -----------
--           5
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvnincrby('abc', 3);
--   kvnincrby
--  -----------
--           8
--  (1 row)
create or replace function kvnincrby(keyname varchar, valuenumber int) returns int as $$
declare
  result int;
begin
  update keyval.numbers set value = (value + valuenumber), updated_at = now() where key = keyname returning value into result;
  if result is null then
    insert into keyval.numbers (key, value, created_at, updated_at) values (keyname, valuenumber, now(), now());
    result := valuenumber;
  end if;
  return result;
end;
$$ language 'plpgsql';

-- KVNINCR: Increments the value stored at the key by 1.
--          If the key does not have a value, the new value becomes 1 instead.
--  ARG1: keyname varchar
--  RTRN: int
--
-- EXAMPLE 1:
--  select * from kvincr('abc');
--   kvnincr
--  ---------
--         9
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvincr('nonexistent');
--   kvnincr
--  ---------
--         1
--  (1 row)
create or replace function kvnincr(keyname varchar) returns int as $$
begin
  return kvnincrby(keyname, 1);
end;
$$ language 'plpgsql';

-- KVNDECRBY: Decrements the value stored at the key by the number given.
--            If the key does not have a value, the number given becomes the new negative value instead.
--  ARG1: keyname varchar
--  ARG2: valuenumber int
--  RTRN: int
--
-- EXAMPLE 1:
--  select * from kvndecrby('abc', 2);
--   kvndecrby
--  -----------
--           7
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvndecrby('nonexistent', 3);
--   kvndecrby
--  -----------
--          -3
--  (1 row)
create or replace function kvndecrby(keyname varchar, valuenumber int) returns int as $$
begin
  return kvnincrby(keyname, (valuenumber - (valuenumber * 2)));
end;
$$ language 'plpgsql';

-- KVNDECR: Decrements the value stored at the key by 1.
--          If the key does not have a value, the new value becomes -1 instead.
--  ARG1: keyname varchar
--  RTRN: int
--
-- EXAMPLE 1:
--  select * from kvndecr('abc');
--   kvndecr
--  ---------
--         6
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvndecr('nonexistent');
--   kvndecr
--  ---------
--        -1
--  (1 row)
create or replace function kvndecr(keyname varchar) returns int as $$
begin
  return kvndecrby(keyname, 1);
end;
$$ language 'plpgsql';

-- KVNMGET: Returns the key and number values of all specified keys.
--          For any keys specified that do not have a string value, NULL is returned instead.
--  ARG1: keynames varchar[]
--  RTRN: table(key varchar, value int)
--
-- EXAMPLE 1:
--  select * from kvnmget(array['abc', 'nonexistent', 'def', 'nonexistent']);
--       key     | value
--  -------------+--------
--   abc         |      3
--   nonexistent | <NULL>
--   def         |      7
--   nonexistent | <NULL>
--  (4 rows)
create or replace function kvnmget(keynames varchar[]) returns table(key varchar, value int) as $$
begin
  return query with
    keys as (select unnest(keynames))
    select keys.unnest, numbers.value from keys
    left outer join keyval.numbers on numbers.key = keys.unnest;
end;
$$ language 'plpgsql';

-- KVNMSET: Sets all keys to their respective number values.
--          For any keys that already hold a value, they will be overwritten instead.
--          If more keys then values are given, an error will be raised.
--          If more values then keys are given, an error will be raised.
--  ARG1: keynames varchar[]
--  ARG2: valuenumbers int[]
--  RTRN: void
--
-- EXAMPLE 1:
--  select * from kvnmset(array['a', 'b', 'c'], array[1, 2, 3]);
--   kvnmset
--  ---------
--
--  (1 row)
create or replace function kvnmset(keynames varchar[], valuenumbers int[]) returns void as $$
declare
  keyname varchar;
  i int := 1;
begin
  if array_length(keynames, 1) = array_length(valuenumbers, 1) then
    foreach keyname in array keynames loop
      update keyval.numbers set value = valuenumbers[i], updated_at = now() where key = keyname;
      if not found then
        insert into keyval.numbers (key, value, created_at, updated_at) values (keyname, valuenumbers[i], now(), now());
      end if;
      i := i + 1;
    end loop;
  else
    raise exception 'The size of the "keynames" and "valuenumbers" arguments must match!';
  end if;
end;
$$ language 'plpgsql';

-- KVNMSETNX: Sets all keys to their respective number values, if all of the keys specified have not been set already and returns TRUE.
--            If any of the specified keys have already been set to a number value, this function will do nothing and return FALSE instead.
--            If more keys then values are given, an error will be raised.
--            If more values then keys are given, an error will be raised.
--  ARG1: keynames varchar[]
--  ARG2: valuenumbers int[]
--  RTRN: boolean
--
-- EXAMPLE 1:
--  select * from kvnmsetnx(array['a', 'b', 'c'], array[1, 2, 3]);
--   kvnmsetnx
--  -----------
--   t
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvnmsetnx(array['a', 'd', 'e'], array[4, 5, 6]);
--   kvnmsetnx
--  -----------
--   f
--  (1 row)
create or replace function kvnmsetnx(keynames varchar[], valuenumbers int[]) returns boolean as $$
declare
  result boolean := true;
  keyname varchar;
  i int := 1;
begin
  if array_length(keynames, 1) = array_length(valuenumbers, 1) then
    perform key from keyval.numbers where key = any(keynames) limit 1;
    if found then
      result := false;
    else
      foreach keyname in array keynames loop
        update keyval.numbers set value = valuenumbers[i], updated_at = now() where key = keyname;
        if not found then
          insert into keyval.numbers (key, value, created_at, updated_at) values (keyname, valuenumbers[i], now(), now());
        end if;
        i := i + 1;
      end loop;
    end if;
  else
    raise exception 'The size of the "keynames" and "valuenumbers" arguments must match!';
  end if;
  return result;
end;
$$ language 'plpgsql';

-- KVNMSETNX: Sets all keys to their respective number values, if all of the keys specified have not been set already.
--            If any of the specified keys have already been set to a number value, this function will do nothing and raise an error.
--            If more keys then values are given, an error will be raised.
--            If more values then keys are given, an error will be raised.
--  ARG1: keynames varchar[]
--  ARG2: valuenumbers int[]
--  RTRN: void
--
-- EXAMPLE 1:
--  select * from kvnmsetnxe(array['a', 'b', 'c'], array[1, 2, 3]);
--   kvnmsetnxe
--  ------------
--
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvnmsetnxe(array['a', 'd', 'e'], array[4, 5, 6]);
--  ERROR:  One or more of the keynames provided already exist!
create or replace function kvnmsetnxe(keynames varchar[], valuenumbers int[]) returns void as $$
begin
  if not kvnmsetnx(keynames, valuenumbers) then
    raise exception 'One or more of the keynames provided already exist!';
  end if;
end;
$$ language 'plpgsql';

-- KVNSET: Sets the key to hold a number value.
--         If key already holds a value, it is overwritten instead.
--  ARG1: keyname varchar
--  ARG2: valuestring int
--  RTRN: void
--
-- EXAMPLE 1:
--  select * from kvnset('abc', 'hello world');
--   kvnset
--  --------
--
--  (1 row)
create or replace function kvnset(keyname varchar, valuestring int) returns void as $$
begin
  update keyval.numbers set value = valuestring, updated_at = now() where key = keyname;
  if not found then
    insert into keyval.numbers (key, value, created_at, updated_at) values (keyname, valuestring, now(), now());
  end if;
end;
$$ language 'plpgsql';

-- KVNSETNX: Sets the key to hold a number value, if the key does not exist, and returns TRUE.
--           If the key already has a value set, no change is made and FALSE is returned instead.
--  ARG1: keyname varchar
--  ARG2: valuestring int
--  RTRN: boolean
--
-- EXAMPLE 1:
--  select * from kvnsetnx('abc', 3);
--   kvnsetnx
--  ----------
--   t
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvnsetnx('abc', 7);
--   kvnsetnx
--  ----------
--   f
--  (1 row)
create or replace function kvnsetnx(keyname varchar, valuestring int) returns boolean as $$
declare
  result boolean := true;
begin
  begin
    insert into keyval.numbers (key, value, created_at, updated_at) values (keyname, valuestring, now(), now());
  exception when unique_violation then
    result := false;
  end;
  return result;
end;
$$ language 'plpgsql';

-- KVNSETNXE: Sets the key to hold a number value, if the key does not exist.
--            If the key already has a value set, no change is made and an raise an error.
--  ARG1: keyname varchar
--  ARG2: valuenumber int
--  RTRN: boolean
--
-- EXAMPLE 1:
--  select * from kvnsetnxe('abc', 3);
--   kvnsetnxe
--  -----------
--
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvnsetnxe('abc', 7);
--  ERROR:  The keyname provided already exists!
create or replace function kvnsetnxe(keyname varchar, valuenumber int) returns void as $$
begin
  if not kvnsetnx(keyname, valuenumber) then
    raise exception 'The keyname provided already exists!';
  end if;
end;
$$ language 'plpgsql';

-----------------------
-- Start List Functions
-----------------------

kvldel(keyname varchar) returns boolean
kvldele(keyname varchar) returns void

kvlindex(keyname varchar, valuestring text) returns int
kvlinsert(keyname varchar, valuestring text, listindex int) returns void
kvllen(keyname varchar) returns int

kvlget(keyname varchar, listindex int) returns text
kvlgetall(keyname varchar) returns text
kvlgetrange(keyname varchar, lowerindex int, upperindex int) returns table (index int, value text)

-- KVLLPOP: Removes and returns the first string value from the list.
--          If nothing is stored at the key, NULL is returned instead.
--  ARG1: keyname varchar
--  RTRN: text
--
-- EXAMPLE 1:
--  select * from kvllpop('abc');
--    kvllpop
--  ------------
--   last value
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvllpop('abc');
--   kvllpop
--  ---------
--   <NULL>
--  (1 row)
create or replace function kvllpop(keyname varchar) returns text as $$
declare
  isempty boolean;
  result text;
begin
  update keyval.lists set value = lists.value[2:(array_length(lists.value, 1))], updated_at = now()
    from keyval.lists temp
    where lists.key = temp.key
      and lists.key = keyname
    returning array_length(lists.value, 1) is null, temp.value[1] into isempty, result;
  if not found then
    result := null;
  end if;

  if isempty then
    delete from keyval.lists where key = keyname;
  end if;

  return result;
end;
$$ language 'plpgsql';

-- KVLLPOPE: Removes and returns the first string value from the list.
--           If nothing is stored at the key, an error is raised instead.
--  ARG1: keyname varchar
--  RTRN: text
--
-- EXAMPLE 1:
--  select * from kvllpope('abc');
--    kvllpope
--  ------------
--   last value
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvllpope('abc');
--  ERROR:  The keyname provided does not exist!
create or replace function kvllpope(keyname varchar) returns text as $$
declare
  result text;
begin
  select kvllpop(keyname) into result;
  if result is null then
    raise exception 'The keyname provided does not exist!';
  end if;
  return result;
end;
$$ language 'plpgsql';

-- KVLLPUSH: Inserts a new string value at the begining of the list, and returns the lengh of the new list.
--           If nothing is stored at the key, a new list is created with the specified value and 1 will be returned.
--  ARG1: keyname varchar
--  ARG2: valuestring text
--  RTRN: int
--
-- EXAMPLE 1:
--  select * from kvllpush('abc', 'one');
--   kvllpush
--  ----------
--          1
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvllpush('abc', 'two');
--   kvllpush
--  ----------
--          2
--  (1 row)
create or replace function kvllpush(keyname varchar, valuestring text) returns int as $$
declare
  result int;
begin
  update keyval.lists set value = (valuestring || value), updated_at = now() where key = keyname returning array_length(value, 1) into result;
  if not found then
    insert into keyval.lists (key, value, created_at, updated_at) values (keyname, array[valuestring], now(), now());
    result := 1;
  end if;
  return result;
end;
$$ language 'plpgsql';

-- KVLLPUSHNX: Inserts a new string value at the begining of the list, if the string value does not yet exist and returns TRUE.
--             If the string value already exists in the list, this function will do nothing and return FALSE instead.
--             If nothing is stored at the key, a new list is created with the specified value and TRUE will be returned.
--  ARG1: keyname varchar
--  ARG2: valuestring text
--  RTRN: boolean
--
-- EXAMPLE 1:
--  select * from kvllpushnx('abc', 'one');
--   kvllpushnx
--  ------------
--            t
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvllpushnx('abc', 'one');
--   kvllpushnx
--  ------------
--            f
--  (1 row)
create or replace function kvllpushnx(keyname varchar, valuestring text) returns boolean as $$
declare
  result boolean := true;
begin
  begin
    insert into keyval.lists (key, value, created_at, updated_at) values (keyname, array[valuestring], now(), now());
  exception when unique_violation then
    update keyval.lists set value = (valuestring || value), updated_at = now() where key = keyname and not value @> array[valuestring];
    if not found then
      result := false;
    end if;
  end;
  return result;
end;
$$ language 'plpgsql';

-- KVLLPUSHNXE: Inserts a new string value at the begining of the list, if the string value does not yet exist.
--              If the string value already exists in the list, this function will do nothing and raise an error instead.
--              If nothing is stored at the key, a new list is created with the specified value.
--  ARG1: keyname varchar
--  ARG2: valuestring text
--  RTRN: void
--
-- EXAMPLE 1:
--  select * from kvllpushnxe('abc', 'one');
--   kvllpushnxe
--  -------------
--             t
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvllpushnxe('abc', 'one');
--  ERROR:  The valuestring provided already exists in the list at that keyname!
create or replace function kvllpushnxe(keyname varchar, valuestring text) returns void as $$
begin
  if not kvllpushnx(keyname, valuestring) then
    raise exception 'The valuestring provided already exists in the list at that keyname';
  end if;
end;
$$ language 'plpgsql';

-- KVLRPOP: Removes and returns the last string value from the list.
--          If nothing is stored at the key, NULL is returned instead.
--  ARG1: keyname varchar
--  RTRN: text
--
-- EXAMPLE 1:
--  select * from kvlrpop('abc');
--    kvlrpop
--  ------------
--   last value
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvlrpop('abc');
--   kvlrpop
--  ---------
--   <NULL>
--  (1 row)
create or replace function kvlrpop(keyname varchar) returns text as $$
declare
  isempty boolean;
  result text;
begin
  update keyval.lists set value = lists.value[1:(array_length(lists.value, 1) - 1)], updated_at = now()
    from keyval.lists temp
    where lists.key = temp.key
      and lists.key = keyname
    returning array_length(lists.value, 1) is null, temp.value[array_length(temp.value, 1)] into isempty, result;
  if not found then
    result := null;
  end if;

  if isempty then
    delete from keyval.lists where key = keyname;
  end if;

  return result;
end;
$$ language 'plpgsql';

-- KVLRPOPE: Removes and returns the first string value from the list.
--           If nothing is stored at the key, an error is raised instead.
--  ARG1: keyname varchar
--  RTRN: text
--
-- EXAMPLE 1:
--  select * from kvlrpope('abc');
--    kvlrpope
--  ------------
--   last value
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvlrpope('abc');
--  ERROR:  The keyname provided does not exist!
create or replace function kvlrpope(keyname varchar) returns text as $$
declare
  result text;
begin
  select kvlrpop(keyname) into result;
  if result is null then
    raise exception 'The keyname provided does not exist!';
  end if;
  return result;
end;
$$ language 'plpgsql';

-- KVLRPUSH: Inserts a new string value at the end of the list, and returns the lengh of the new list.
--           If nothing is stored at the key, a new list is created with the specified value and 1 will be returned.
--  ARG1: keyname varchar
--  ARG2: valuestring text
--  RTRN: int
--
-- EXAMPLE 1:
--  select * from kvlrpush('abc', 'one');
--   kvlrpush
--  ----------
--          1
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvlrpush('abc', 'two');
--   kvlrpush
--  ----------
--          2
--  (1 row)
create or replace function kvlrpush(keyname varchar, valuestring text) returns int as $$
declare
  result int;
begin
  update keyval.lists set value = (value || valuestring), updated_at = now() where key = keyname returning array_length(value, 1) into result;
  if not found then
    insert into keyval.lists (key, value, created_at, updated_at) values (keyname, array[valuestring], now(), now());
    result := 1;
  end if;
  return result;
end;
$$ language 'plpgsql';

-- KVLRPUSHNX: Inserts a new string value at the end of the list, if the string value does not yet exist and returns TRUE.
--             If the string value already exists in the list, this function will do nothing and return FALSE instead.
--             If nothing is stored at the key, a new list is created with the specified value and TRUE will be returned.
--  ARG1: keyname varchar
--  ARG2: valuestring text
--  RTRN: boolean
--
-- EXAMPLE 1:
--  select * from kvlrpushnx('abc', 'one');
--   kvlrpushnx
--  ------------
--            t
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvlrpushnx('abc', 'one');
--   kvlrpushnx
--  ------------
--            f
--  (1 row)
create or replace function kvlrpushnx(keyname varchar, valuestring text) returns boolean as $$
declare
  result boolean := true;
begin
  begin
    insert into keyval.lists (key, value, created_at, updated_at) values (keyname, array[valuestring], now(), now());
  exception when unique_violation then
    update keyval.lists set value = (value || valuestring), updated_at = now() where key = keyname and not value @> array[valuestring];
    if not found then
      result := false;
    end if;
  end;
  return result;
end;
$$ language 'plpgsql';

-- KVLRPUSHNXE: Inserts a new string value at the end of the list, if the string value does not yet exist.
--              If the string value already exists in the list, this function will do nothing and raise an error instead.
--              If nothing is stored at the key, a new list is created with the specified value.
--  ARG1: keyname varchar
--  ARG2: valuestring text
--  RTRN: void
--
-- EXAMPLE 1:
--  select * from kvlrpushnxe('abc', 'one');
--   kvlrpushnxe
--  -------------
--             t
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvlrpushnxe('abc', 'one');
--  ERROR:  The valuestring provided already exists in the list at that keyname!
create or replace function kvlrpushnxe(keyname varchar, valuestring text) returns void as $$
begin
  if not kvlrpushnx(keyname, valuestring) then
    raise exception 'The valuestring provided already exists in the list at that keyname';
  end if;
end;
$$ language 'plpgsql';

kvlreml(keyname varchar, valuestring text) returns boolean
kvlremle(keyname varchar, valuestring text) returns void
kvlremr(keyname varchar, valuestring text) returns boolean
kvlremre(keyname varchar, valuestring text) returns void
kvlremall(keyname varchar, valuestring text) returns boolean
kvlremalle(keyname varchar, valuestring text) returns void

-----------------------
-- Start Hash Functions
-----------------------

-- KVHDEL: Deletes the string value held by hash field stored at the key and returns TRUE.
--         If the hash field or the key does not exist, FALSE is returned instead.
--  ARG1: keyname varchar
--  ARG2: fieldname text
--  RTRN: boolean
--
-- EXAMPLE 1:
--  select * from kvhdel('greeting', 'austin');
--   kvhdel
--  --------
--   t
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvhdel('greeting', 'nonexistent');
--
--   kvhdel
--  --------
--   f
--  (1 row)
create or replace function kvhdel(keyname varchar, fieldname text) returns boolean as $$
declare
  hashvalue hstore;
  result boolean := false;
begin
  select value from keyval.hashes where key = keyname into hashvalue;
  if found then
    if (hashvalue ? fieldname) = true then
      result := true;
      if array_length(akeys(hashvalue), 1) > 1 then
        update keyval.hashes set value = (value - fieldname), updated_at = now() where key = keyname;
      else
        delete from keyval.hashes where key = keyname;
      end if;
    end if;
  end if;
  return result;
end;
$$ language 'plpgsql';

-- KVHDELE: Deletes the string value held by hash field stored at the key.
--         If the hash field or the key does not exist, an error is raised instead.
--  ARG1: keyname varchar
--  RTRN: void
--
-- EXAMPLE 1:
--  select * from kvhdele('abc');
--   kvhdele
--  --------
--
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvhdele('nonexistent');
--  ERROR:  The keyname or fieldname provided does not exists!
create or replace function kvhdele(keyname varchar, fieldname text) returns void as $$
begin
  if not kvhdel(keyname) then
    raise exception 'The keyname or fieldname provided does not exist!';
  end if;
end;
$$ language 'plpgsql';

-- KVHDELALL: Deletes all hash fields stored at the key and returns TRUE.
--            If the key does not exist, FALSE is returned instead.
--  ARG1: keyname varchar
--  RTRN: boolean
--
-- EXAMPLE 1:
--  select * from kvhdelall('abc');
--   kvhdelall
--  -----------
--   t
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvhdelall('nonexistent');
--
--   kvhdelall
--  -----------
--   f
--  (1 row)
create or replace function kvhdelall(keyname varchar) returns boolean as $$
declare
  result boolean := false;
begin
  delete from keyval.hashes where key = keyname;
  if found then
    result := true;
  end if;
  return result;
end;
$$ language 'plpgsql';

-- KVDELALLE: Deletes all hash fields stored at the key.
--            If the key does not exist, an error is raised instead.
--  ARG1: keyname varchar
--  RTRN: boolean
--
-- EXAMPLE 1:
--  select * from kvhdelalle('abc');
--   kvhdelalle
--  ------------
--
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvhdelalle('nonexistent');
--  ERROR:  The keyname provided does not exist!
create or replace function kvhdelalle(keyname varchar) returns void as $$
begin
  if not kvhdelall(keyname) then
    raise exception 'The keyname provided does not exist!';
  end if;
end;
$$ language 'plpgsql';

-- KVHEXIST: Returns TRUE if the hash field at the key has a value.
--           If the field or the key does not exist, FALSE is returned instead.
--  ARG1: keyname varchar
--  ARG2: fieldname text
--  RTRN: boolean
--
-- EXAMPLE 1:
--  select * from kvhexists('greeting', 'austin');
--   kvhexists
--  -----------
--   t
--  (1 row)
--
-- EXAMPLE 1:
--  select * from kvhexists('greeting', 'nonexistent');
--   kvhexists
--  -----------
--   f
--  (1 row)
create or replace function kvhexists(keyname varchar, fieldname text) returns boolean as $$
declare
  result boolean;
begin
  select coalesce((exist(value, fieldname)), false) from keyval.hashes where key = keyname into result;
  if not found then
    result := false;
  end if;
  return result;
end;
$$ language 'plpgsql';

-- KVHEXISTE: Returns nothing if the hash field at the key has a value.
--            If the field or the key does not exist, an error is raised instead.
--  ARG1: keyname varchar
--  ARG2: fieldname text
--  RTRN: void
--
-- EXAMPLE 1:
--  select * from kvhexistse('greeting', 'austin');
--   kvhexistse
--  -----------
--
--  (1 row)
--
-- EXAMPLE 1:
--  select * from kvhexistse('greeting', 'nonexistent');
--  ERROR:  The keyname or fieldname provided does not exist!
create or replace function kvhexistse(keyname varchar, fieldname text) returns void as $$
begin
  if not kvhexists(keyname, fieldname) then
    raise exception 'The keyname or fieldname provided does not exist!';
  end if;
end;
$$ language 'plpgsql';

-- KVHGET: Returns the string value of the field stored at the key.
--         If the field or the key does not exist, NULL is returned instead.
--  ARG1: keyname varchar
--  ARG2: fieldname text
--  RTRN: text
--
-- EXAMPLE 1:
--  select * from kvhget('greeting', 'austin');
--       kvhget
--  ---------------
--   howdy partner
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvhget('greeting', 'nonexistent');
--   kvhget
--  --------
--   <NULL>
--  (1 row)
--
-- EXAMPLE 3:
--  select * from kvhget('nonexistent', 'nonexistent');
--   kvhget
--  --------
--   <NULL>
--  (1 row)
create or replace function kvhget(keyname varchar, fieldname text) returns text as $$
declare
  result text;
begin
  select (value -> fieldname) from keyval.hashes where key = keyname into result;
  return result;
end;
$$ language 'plpgsql';

-- KVHGETSET: Sets the hash field at the key to hold a string value and returns the old value stored.
--            If no value was previously stored, NULL is returned instead.
--  ARG1: keyname varchar
--  ARG2: fieldname text
--  ARG3: valuestring text
--  RTRN: text
--
-- EXAMPLE 1:
--  select * from kvhgetset('abc', 'greeting', 'hello world');
--   kvhgetset
--  -----------
--
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvhgetset('abc', 'greeting', 'howdy partner');
--    kvhgetset
--  -------------
--   hello world
--  (1 row)
create or replace function kvhgetset(keyname varchar, fieldname text, valuestring text) returns text as $$
declare
  result text;
begin
  select (value -> fieldname) from keyval.hashes where key = keyname into result;
  if found then
    update keyval.hashes set value = (value || hstore(fieldname, valuestring)), updated_at = now() where key = keyname;
  else
    insert into keyval.hashes (key, value, created_at, updated_at) values (keyname, hstore(fieldname, valuestring), now(), now());
  end if;
  return result;
end;
$$ language 'plpgsql';

-- KVHKEYS: Returns a list of hash field names and field values stored by the key.
--  ARG1: keyname varchar
--  RTRN: table(name, value)
--
-- EXAMPLE 1:
--  select * from kvhgetall('greeting');
--     name   |     value
--  ----------+---------------
--   austin   | howdy partner
--   tokyo    | moshi moshi
--   brooklyn | hey bud
--  (3 rows)
--
-- EXAMPLE 2:
--  select * from kvhvals('nonexistent');
--   value
--  -------
--  (0 rows)
create or replace function kvhgetall(keyname varchar) returns table(name text, value text) as $$
begin
  return query select skeys(hashes.value), svals(hashes.value) from keyval.hashes where hashes.key = keyname;
end;
$$ language 'plpgsql';

-- KVHNAMES: Returns a list of hash field names stored by the key.
--  ARG1: keyname varchar
--  RTRN: table(key)
--
-- EXAMPLE 1:
--  select * from kvhnames('greeting');
--     name
--  ----------
--   austin
--   tokyo
--   brooklyn
--  (3 rows)
--
-- EXAMPLE 2:
--  select * from kvhnames('nonexistent');
--   name
--  -------
--  (0 rows)
create or replace function kvhnames(keyname varchar) returns table(name text) as $$
begin
  return query select skeys(value) from keyval.hashes where hashes.key = keyname;
end;
$$ language 'plpgsql';

-- KVHMGET: Returns the hash field name and string values of all specified field names stored at the key.
--          For any field names specified that do not have a string value, NULL is returned instead.
--  ARG1: keyname varchar
--  ARG2: fieldnames text[]
--  RTRN: table(name, value)
--
-- EXAMPLE 1:
--  select * from kvhmget('greeting', array['austin', 'nonexistent', 'brooklyn']);
--       name    |     value
--  -------------+---------------
--   austin      | howdy partner
--   nonexistent | <NULL>
--   brooklyn    | hey buddy
--   (3 rows)
create or replace function kvhmget(keyname varchar, fieldnames text[]) returns table(name text, value text) as $$
begin
  return query with
    pairs as (select skeys(hashes.value), svals(hashes.value) from keyval.hashes where hashes.key = keyname),
    names as (select unnest(fieldnames))
    select unnest, svals from names left outer join pairs on pairs.skeys = names.unnest;
end;
$$ language 'plpgsql';

-- KVHMSET: Sets all hash fields stored at the key to their respective string values.
--          For any hash fields that already hold a value, they will be overwritten instead.
--          If more keys then values are given, an error will be raised.
--          If more values then keys are given, an error will be raised.
--  ARG1: keynames varchar[]
--  ARG2: fieldnames text[]
--  ARG3: valuestrings text[]
--  RTRN: void
--
-- EXAMPLE 1:
--  select * from kvhmset('greeting', array['austin', 'brooklyn', 'tokyo'], array['howdy partner', 'hey buddy', 'moshi moshi']);
--   kvhmset
--  ---------
--
--  (1 row)
create or replace function kvhmset(keyname varchar, fieldnames text[], valuestrings text[]) returns void as $$
begin
  begin
    update keyval.hashes set value = (value || hstore(fieldnames, valuestrings)), updated_at = now() where key = keyname;
    if not found then
      insert into keyval.hashes (key, value, created_at, updated_at) values (keyname, hstore(fieldnames, valuestrings), now(), now());
    end if;
  exception when array_subscript_error then
    raise exception 'The size of the "fieldnames" and "valuestrings" arguments must match!';
  end;
end;
$$ language 'plpgsql';

-- KVHMSETNX: Sets all hash fields stored on the key to their respective string values,
--              if all of the fields specified have not been set already and returns TRUE.
--            If any of the specified hash fields have already been set to a string value, this function will do nothing and return FALSE instead.
--            If more keys then values are given, an error will be raised.
--            If more values then keys are given, an error will be raised.
--  ARG1: keyname varchar
--  ARG2: fieldnames text[]
--  ARG3: valuestrings text[]
--  RTRN: boolean
--
-- EXAMPLE 1:
--  select * from kvhmsetnx('greeting', array['austin', 'brooklyn', 'tokyo'], array['howdy partner', 'hey buddy', 'moshi moshi']);
--   kvhmsetnx
--  -----------
--   t
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvhmsetnx('greeting', array['austin', 'boston', 'san fran'], array['howdy partner', 'its cold', 'hello world']);
--   kvhmsetnx
--  -----------
--   f
--  (1 row)
create or replace function kvhmsetnx(keyname varchar, fieldnames text[], valuestrings text[]) returns boolean as $$
declare
  result boolean := true;
  canupdate boolean;
begin
  begin
    select not (value ?| fieldnames) from keyval.hashes where key = keyname into canupdate;
    if not found then
      insert into keyval.hashes (key, value, created_at, updated_at) values (keyname, hstore(fieldnames, valuestrings), now(), now());
    else
      if canupdate then
        update keyval.hashes set value = (value || hstore(fieldnames, valuestrings)), updated_at = now() where key = keyname;
      else
        result := false;
      end if;
    end if;
  exception when array_subscript_error then
    raise exception 'The size of the "fieldnames" and "valuestrings" arguments must match!';
  end;
  return result;
end;
$$ language 'plpgsql';

-- KVHMSETNXE: Sets all hash fields stored on the key to their respective string values,
--               if all of the fields specified have not been set already.
--             If any of the specified hash fields have already been set to a string value, this function will do nothing and raise an error.
--             If more keys then values are given, an error will be raised.
--             If more values then keys are given, an error will be raised.
--  ARG1: keyname varchar
--  ARG2: fieldnames text[]
--  ARG3: valuestrings text[]
--  RTRN: void
--
-- EXAMPLE 1:
--  select * from kvhmsetnxe('greeting', array['austin', 'brooklyn', 'tokyo'], array['howdy partner', 'hey buddy', 'moshi moshi']);
--   kvhmsetnxe
--  ------------
--
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvhmsetnxe('greeting', array['austin', 'boston', 'san fran'], array['howdy partner', 'its cold', 'hello world']);
--  ERROR:  One or more of the keynames or fieldnames provided already exist!
create or replace function kvhmsetnxe(keyname varchar, fieldnames text[], valuestrings text[]) returns void as $$
begin
  if not kvhmsetnx(keyname, fieldnames, valuestrings) then
    raise exception 'One or more or fieldnames provided already exist at that keyname!';
  end if;
end;
$$ language 'plpgsql';

-- KVHREPLACE: Replaces the hash fields stored at the key with the specified string values.
--             Any hash fields previously stored will be erased.
--             If more keys then values are given, an error will be raised.
--             If more values then keys are given, an error will be raised.
--  ARG1: keynames varchar[]
--  ARG2: fieldnames text[]
--  ARG3: valuestrings text[]
--  RTRN: void
--
-- EXAMPLE 1:
--  select * from kvhreplace('greeting', array['austin', 'brooklyn', 'tokyo'], array['howdy partner', 'hey buddy', 'moshi moshi']);
--   kvhreplace
--  ------------
--
--  (1 row)
create or replace function kvhreplace(keyname varchar, fieldnames text[], valuestrings text[]) returns void as $$
begin
  begin
    update keyval.hashes set value = hstore(fieldnames, valuestrings), updated_at = now() where key = keyname;
    if not found then
      insert into keyval.hashes (key, value, created_at, updated_at) values (keyname, hstore(fieldnames, valuestrings), now(), now());
    end if;
  exception when array_subscript_error then
    raise exception 'The size of the "fieldnames" and "valuestrings" arguments must match!';
  end;
end;
$$ language 'plpgsql';

-- KVHREPLACENX: Replaces the hash fields stored at the key with the specified string values and returns TRUE.
--               If the key already has hash fields set, no change is made and FALSE is returned instead.
--               If more keys then values are given, an error will be raised.
--               If more values then keys are given, an error will be raised.
--  ARG1: keynames varchar[]
--  ARG2: fieldnames text[]
--  ARG3: valuestrings text[]
--  RTRN: boolean
--
-- EXAMPLE 1:
--  select * from kvreplacenx('greeting', array['austin', 'brooklyn', 'tokyo'], array['howdy partner', 'hey buddy', 'moshi moshi']);
--   kvreplacenx
--  -------------
--   t
--  (1 row)
-- EXAMPLE 1:
--  select * from kvreplacenx('greeting', array['austin', 'brooklyn', 'tokyo'], array['howdy partner', 'hey buddy', 'moshi moshi']);
--   kvreplacenx
--  -------------
--   f
--  (1 row)
create or replace function kvhreplacenx(keyname varchar, fieldnames text[], valuestrings text[]) returns boolean as $$
declare
  result boolean := true;
begin
  begin
    insert into keyval.hashes (key, value, created_at, updated_at) values (keyname, hstore(fieldnames, valuestrings), now(), now());
  exception when unique_violation then
    result := false;
  end;
  return result;
end;
$$ language 'plpgsql';

-- KVHREPLACENXE: Replaces the hash fields stored at the key with the specified string values.
--                If the key already has hash fields set, no change is made and an error raised instead.
--                If more keys then values are given, an error will be raised.
--                If more values then keys are given, an error will be raised.
--  ARG1: keynames varchar[]
--  ARG2: fieldnames text[]
--  ARG3: valuestrings text[]
--  RTRN: void
--
-- EXAMPLE 1:
--  select * from kvreplacenxe('greeting', array['austin', 'brooklyn', 'tokyo'], array['howdy partner', 'hey buddy', 'moshi moshi']);
--   kvreplacenx
--  -------------
--
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvreplacenxe('greeting', array['austin', 'brooklyn', 'tokyo'], array['howdy partner', 'hey buddy', 'moshi moshi']);
--  ERROR:  The keyname provided already exists!
create or replace function kvhreplacenxe(keyname varchar, fieldnames text[], valuestrings text[]) returns void as $$
begin
  if not kvhreplacenx(keyname, fieldnames, valuestrings) then
    raise exception 'The keyname provided already exists!';
  end if;
end;
$$ language 'plpgsql';

-- KVHSET: Sets the hash field at the key to hold a string value.
--         If the hash field or the key already holds a value, it is overwritten instead.
--  ARG1: keyname varchar
--  ARG2: fieldname text
--  ARG3: valuestring text
--  RTRN: void
--
-- EXAMPLE 1:
--  select * from kvhset('abc', 'greeting', 'hello world');
--   kvhset
--  --------
--
--  (1 row)
create or replace function kvhset(keyname varchar, fieldname text, valuestring text) returns void as $$
begin
  update keyval.hashes set value = (value || hstore(fieldname, valuestring)), updated_at = now() where key = keyname;
  if not found then
    insert into keyval.hashes (key, value, created_at, updated_at) values (keyname, hstore(fieldname, valuestring), now(), now());
  end if;
end;
$$ language 'plpgsql';

-- KVHSETNX: Sets the hash field stored at the key to hold a string value, if the hash field does not exist, and returns TRUE.
--           If the hash field already has a value set, no change is made and FALSE is returned instead.
--  ARG1: keyname varchar
--  ARG2: valuestring text
--  RTRN: boolean
--
-- EXAMPLE 1:
--  select * from kvhsetnx('greeting', 'tokyo', 'moshi moshi');
--   kvhsetnx
--  ----------
--   t
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvhsetnx('greeting', 'tokyo', 'ohayo');
--   kvhsetnx
--  ----------
--   f
--  (1 row)
create or replace function kvhsetnx(keyname varchar, fieldname text, valuestring text) returns boolean as $$
declare
  result boolean := true;
begin
  begin
    insert into keyval.hashes (key, value, created_at, updated_at) values (keyname, hstore(fieldname, valuestring), now(), now());
  exception when unique_violation then
    update keyval.hashes set value = (value || hstore(fieldname, valuestring)), updated_at = now() where key = keyname and not value ? fieldname;
    if not found then
      result := false;
    end if;
  end;
  return result;
end;
$$ language 'plpgsql';

-- KVHSETNXE: Sets the hash field stored at the key to hold a string value, if the hash field does not exist, and returns TRUE.
--            If the hash field already has a value set, no change is made and FALSE is returned instead.
--  ARG1: keyname varchar
--  ARG2: valuestring text
--  RTRN: boolean
--
-- EXAMPLE 1:
--  select * from kvhsetnxe('greeting', 'tokyo', 'moshi moshi');
--   kvhsetnxe
--  -----------
--
--  (1 row)
--
-- EXAMPLE 2:
--  select * from kvhsetnxe('greeting', 'tokyo', 'ohayo');
--  ERROR:  The keyname or fieldname provided does not exist!
create or replace function kvhsetnxe(keyname varchar, fieldname text, valuestring text) returns void as $$
begin
  if not kvhsetnx(keyname, fieldname, valuestring) then
    raise exception 'The fieldname provided already exists at that keyname!';
  end if;
end;
$$ language 'plpgsql';

-- KVHVALS: Returns a list of hash field values stored by the key.
--  ARG1: keyname varchar
--  RTRN: table(value)
--
-- EXAMPLE 1:
--  select * from kvhvals('greeting');
--       value
--  ---------------
--   howdy partner
--   moshi moshi
--   hey buddy
--  (3 rows)
--
-- EXAMPLE 2:
--  select * from kvhvals('nonexistent');
--   value
--  -------
--  (0 rows)
create or replace function kvhvals(keyname varchar) returns table(value text) as $$
begin
  return query select svals(value) from keyval.hashes where keyval.hashes.key = keyname;
end;
$$ language 'plpgsql';
