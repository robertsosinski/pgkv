Introduction
============

PGKV is a set of functions that allow Postgres to handle atomic operations in a similar fashon to Redis.  At the moment, PGKV supports strings, numbers, lists and hashes as data types, each of which is stored in a seperate keyspace.  All keyspaces are held in the `keyval` schema.

Installation
------------

The best way to install PGKV is to import the `pgkv.sql` file into the chosen database.  From there, you can interact with the keyspaces using the functions found in the `pgkv.sql` file.

Using PGKV
----------

One function in PGKV is the `kvset` function, which will create a new string key/value if it does not exist, and update it if it does.

    select * from kvset('abc', 'hello world');
     kvset
    -------

    (1 row)

From there, you can get the key/value using the `kvget` function.

    select * from kvget('abc');
        kvget
    -------------
     hello world
    (1 row)


If you wish to only set the key if it does not yet exist, you can use the `kvsetnx` function.

    select * from kvsetnx('abc', 'hello world');
     kvsetnx
    ---------
     f
    (1 row)

As the key has already been set, `false` is returned. However, if the key was not set, `true` would have been returned instead.

With PGKV, you can also perform list operations, such as pushing new values onto the left side of a list with `kvllpush`.

    select * from kvllpush('abc', 'one');
     kvllpush
    ----------
            1
    (1 row)

    select * from kvllpush('abc', 'two');
     kvllpush
    ----------
            2
    (1 row)

And popping then off from the right with `kvrpop`.

    select * from kvlrpop('abc');
     kvlrpop
    ---------
     one
    (1 row)

    select * from kvlrpop('abc');
     kvlrpop
    ---------
     two
    (1 row)

Learning More
-------------

At this moment, the best way to learn about the 70 different functions in PGKV is to browse the documented source file `pgkv.sql`.

Notes
-----

I started making these functions in order to get better PL/pgSQL and at making SQL that works concurrently while preventing race conditions.  I would not recommend using these functions directly in production, but feel free to use them for insperation on solving similar problems you might have.

Also, if you want to use these functions to mimic a non-durable datastore due to speed being more important then the data saved, feel free to modify the table definitions to be unlogged.  You can learn more about that [here](http://wiki.postgresql.org/wiki/What's_new_in_PostgreSQL_9.1#Unlogged_Tables).

Credits
-------

Built by [Robert Sosinski](http://www.robertsosinski.com) and open sourced with a [MIT license](http://github.com/robertsosinski/couch-client/blob/master/LICENSE).