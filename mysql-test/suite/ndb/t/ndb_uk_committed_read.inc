# Committed read through a unique hash index on table $tab.
# Only the hidden unique index row is locked (shared), the base table row
# is read without a lock, so the read neither waits for nor deadlocks with
# a transaction holding the base row.
# Expects connections con1 and con2, and rows (1,1,1),(2,2,2) in $tab.

--echo # $tab: committed read by unique key while another transaction holds
--echo # an exclusive lock on the base row through the primary key
connection con1;
begin;
eval select * from $tab where pk = 1 for update;
connection con2;
eval select * from $tab where uk = 1;
connection con1;
rollback;

--echo # $tab: the hidden unique index row is still locked, so a committed
--echo # read by unique key waits for an uncommitted delete of the row
--echo # (a write locks the index row on every replica, an exclusive read
--echo # only on the replica it was sent to)
connection con1;
begin;
eval delete from $tab where pk = 2;
connection con2;
--error 1205
eval select * from $tab where uk = 2;
connection con1;
rollback;
connection con2;
eval select * from $tab where uk = 2;

--echo # $tab: an uncommitted update of the base row is not seen, the
--echo # committed value is returned without waiting
connection con1;
begin;
eval update $tab set v = v + 100 where pk = 1;
eval select * from $tab where uk = 1;
connection con2;
eval select * from $tab where uk = 1;
connection con1;
rollback;

--echo # $tab: the unique index row of an insert commits before the base
--echo # row (error insert 5160 delays COMMIT of user table operations).
--echo # Reading in that window gives a temporary error, not "no row".
--exec $NDB_MGM -e "ALL ERROR 5160" >> $NDB_TOOLS_OUTPUT
connection con1;
begin;
eval insert into $tab values (3,3,3);
--send commit
connection con2;
--error 1297
eval select * from $tab where uk = 3;
connection con1;
--reap
--exec $NDB_MGM -e "ALL ERROR 0" >> $NDB_TOOLS_OUTPUT
connection con2;
eval select * from $tab where uk = 3;

--echo # $tab: a committed delete is a plain "no row", not a temporary error
connection con1;
eval delete from $tab where pk = 2;
connection con2;
eval select * from $tab where uk = 2;
