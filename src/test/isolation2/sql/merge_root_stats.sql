-----------------------------------------------
-- Reset flags to ignore any changes from tests before running these tests.
set optimizer_trace_fallback=on;
reset optimizer_analyze_root_partition;
reset optimizer_analyze_midlevel_partition;
-- To control running of autovacuum, naptime is set to high value
-- (2147483 = INT_MAX/1000), it is changed to low value when autovacuum
-- is required to trigger.
alter system set autovacuum_naptime = 2147483;
alter system set autovacuum_vacuum_threshold = 10;
alter system set autovacuum_analyze_threshold = 10;
select * from pg_reload_conf();
create extension if not exists gp_inject_fault;
select gp_inject_fault('analyze_finished_one_relation', 'reset', 1);
-----------------------------------------------
-- Case 1 - If an 'Analyzed Partition' is attached/detached to
--     an analyzed table, merging of leaf stats is expected.
--
--         Root  Analyzed
--         Leaf1 Analyzed (as table analyzed)
--         Leaf2 Analyzed
-----------------------------------------------

-- 1. Prepare basic framework
drop table if exists rootTab;
drop table if exists rootTabMid1;
drop table if exists rootTabLeaf1;
drop table if exists rootTabLeaf2;
create table rootTab(a int)  partition by range(a);
create table rootTabMid1(a int) partition by range(a);
create table rootTabLeaf1(a int);
create table rootTabLeaf2(a int);
alter table rootTab attach partition rootTabMid1 for values from (0) to (20);
alter table rootTabMid1 attach partition rootTabLeaf1 for values from (0) to (10);
insert into rootTab select i%10 from generate_series(0,4)i;
analyze rootTab;
select tablename, attname,inherited,histogram_bounds from pg_stats where tablename like 'root%' order by tablename;

-- 2. Update rootTabLeaf1 to check, if it is re-sampled when a new partiton is attached.
-- These should not be present in the root stats, after attach/detach of partition
insert into rootTabLeaf1 select i%10 from generate_series(5,9)i;

-- 3. Analyze rootTabLeaf2 (new partition to be added)
insert into rootTabLeaf2 select i%20 from generate_series(10,19)i;
analyze rootTabLeaf2;
select tablename, attname,inherited,histogram_bounds from pg_stats where tablename like 'root%' order by tablename;

-- 4. Attach new partition, wait for autoanalyze to trigger and check that stats are updated.
select gp_inject_fault('analyze_finished_one_relation', 'skip', '', '', 'roottab', 1, -1, 0, 1);
alter table rootTabMid1 attach partition rootTabLeaf2 for values from (10) to (20);
alter system set autovacuum_naptime = 2;
select * from pg_reload_conf();
select gp_wait_until_triggered_fault('analyze_finished_one_relation', 1, 1);
select gp_inject_fault('analyze_finished_one_relation', 'reset', 1);
alter system set autovacuum_naptime = 2147483;
select * from pg_reload_conf();
select count(*) from roottab;
select tablename, attname,inherited,histogram_bounds from pg_stats where tablename like 'root%' order by tablename;

--5. Detach Partition (Leaf2), wait for autoanalyze to trigger and check that stats are updated.
select gp_inject_fault('analyze_finished_one_relation', 'skip', '', '', 'roottab', 1, -1, 0, 1);
alter table rootTabMid1 detach partition rootTabLeaf2;
alter system set autovacuum_naptime = 2;
select * from pg_reload_conf();
select gp_wait_until_triggered_fault('analyze_finished_one_relation', 1, 1);
select gp_inject_fault('analyze_finished_one_relation', 'reset', 1);
alter system set autovacuum_naptime = 2147483;
select * from pg_reload_conf();
select count(*) from roottab;
select tablename, attname,inherited,histogram_bounds from pg_stats where tablename like 'root%' order by tablename;

-----------------------------------------------
-- Case 2 - If an 'Un-Analyzed Partition' is attached/detached
--     to an analyzed table, and it is also not analyzed by autovacuum
--     then, merging of leaf stats is not expected.
--
--         Root  Analyzed
--         Leaf1 Analyzed (as table analyzed)
--         Leaf2 Not Analyzed
-----------------------------------------------
-- 1. Prepare basic framework
drop table if exists rootTab;
drop table if exists rootTabLeaf1;
drop table if exists rootTabLeaf2;
create table rootTab(a int)  partition by range(a);
create table rootTabMid1(a int) partition by range(a);
create table rootTabLeaf1(a int);
create table rootTabLeaf2(a int);
alter table rootTab attach partition rootTabMid1 for values from (0) to (20);
alter table rootTabMid1 attach partition rootTabLeaf1 for values from (0) to (10);
insert into rootTab select i%10 from generate_series(0,4)i;
analyze rootTab;
select tablename, attname,inherited,histogram_bounds from pg_stats where tablename like 'root%' order by tablename;

-- 2. Update rootTabLeaf1 to check, if it is re-sampled when a new partiton is attached
-- These should not be present in the root stats, after attach/detach of partition
insert into rootTabLeaf1 select i%10 from generate_series(5,9)i;

-- 3.Only Insert, not analyze the new partition to be added
insert into rootTabLeaf2 select i%20 from generate_series(10,19)i;

-- 4. Attach new partition, and check that stats are not updated as partition was not analyzed
-- This partition was not auto analyzed as the number of rows inserted in the partition are
-- less than autovacuum_analyze_threshold. Note that, auto analyze did trigger for this root.
-- It can be checked with a query on pg_stat_operations
-- select objname, actionname, subtype from pg_stat_operations where objname like 'root%' order by statime ASC;
select gp_inject_fault('analyze_finished_one_relation', 'skip', '', '', 'roottab', 1, -1, 0, 1);
alter table rootTabMid1 attach partition rootTabLeaf2 for values from (10) to (20);
alter system set autovacuum_naptime = 2;
select * from pg_reload_conf();
select gp_wait_until_triggered_fault('analyze_finished_one_relation', 1, 1);
select gp_inject_fault('analyze_finished_one_relation', 'reset', 1);
alter system set autovacuum_naptime = 2147483;
select * from pg_reload_conf();
select count(*) from roottab;
select tablename, attname,inherited,histogram_bounds from pg_stats where tablename like 'root%' order by tablename;

-- 5. Detach Partition (Leaf2) and check that stats are not updated.
select gp_inject_fault('analyze_finished_one_relation', 'skip', '', '', 'roottab', 1, -1, 0, 1);
alter table rootTabMid1 detach partition rootTabLeaf2;
alter system set autovacuum_naptime = 2;
select * from pg_reload_conf();
select gp_wait_until_triggered_fault('analyze_finished_one_relation', 1, 1);
select gp_inject_fault('analyze_finished_one_relation', 'reset', 1);
alter system set autovacuum_naptime = 2147483;
select * from pg_reload_conf();
select count(*) from roottab;
select tablename, attname,inherited,histogram_bounds from pg_stats where tablename like 'root%' order by tablename;


-----------------------------------------------
-- Case 3 - If the table is not analyzed, but all the existing leafs are
--     analyzed, then if an 'Analyzed Partition' is attached/detached,
--     merging of leaf stats is expected
--
--         Root  Not Analyzed
--         Leaf1 Analyzed
--         Leaf2 Analyzed
-----------------------------------------------
-- 1. Prepare basic framework
drop table if exists rootTab;
drop table if exists rootTabLeaf1;
drop table if exists rootTabLeaf2;
create table rootTab(a int)  partition by range(a);
create table rootTabMid1(a int) partition by range(a);
alter table rootTab attach partition rootTabMid1 for values from (0) to (20);
create table rootTabLeaf1(a int);
insert into rootTabLeaf1 select i%10 from generate_series(0,4)i;
analyze rootTabLeaf1;
select tablename, attname,inherited,histogram_bounds from pg_stats where tablename like 'root%' order by tablename;

-- 2. Attach analyed partition(Leaf1), wait for autoanalyze to trigger and check that root stats are updated.
select gp_inject_fault('analyze_finished_one_relation', 'skip', '', '', 'roottab', 1, -1, 0, 1);
alter table rootTabMid1 attach partition rootTabLeaf1 for values from (0) to (10);
alter system set autovacuum_naptime = 2;
select * from pg_reload_conf();
select gp_wait_until_triggered_fault('analyze_finished_one_relation', 1, 1);
select gp_inject_fault('analyze_finished_one_relation', 'reset', 1);
alter system set autovacuum_naptime = 2147483;
select * from pg_reload_conf();
select count(*) from roottab;
select tablename, attname,inherited,histogram_bounds from pg_stats where tablename like 'root%' order by tablename;

-- 3. Analyze the new partition (Leaf2) to be added
create table rootTabLeaf2(a int);
insert into rootTabLeaf2 select i%20 from generate_series(10,19)i;
analyze rootTabLeaf2;
select tablename, attname,inherited,histogram_bounds from pg_stats where tablename like 'root%' order by tablename;

-- 4. Update rootTabLeaf1 to check, if it is analyzed when a new partiton is attached.
-- These should not be present in the root stats, after attach/detach of partition
insert into rootTabLeaf1 select i%10 from generate_series(5,9)i;

-- 5. Attach new partition, wait for autoanalyze to trigger and check that root stats are updated.
select gp_inject_fault('analyze_finished_one_relation', 'skip', '', '', 'roottab', 1, -1, 0, 1);
alter table rootTabMid1 attach partition rootTabLeaf2 for values from (10) to (20);
alter system set autovacuum_naptime = 2;
select * from pg_reload_conf();
select gp_wait_until_triggered_fault('analyze_finished_one_relation', 1, 1);
select gp_inject_fault('analyze_finished_one_relation', 'reset', 1);
alter system set autovacuum_naptime = 2147483;
select * from pg_reload_conf();
select count(*) from roottab;
select tablename, attname,inherited,histogram_bounds from pg_stats where tablename like 'root%' order by tablename;

-- 6. Detach Partition (Leaf2) and check that root stats are updated.
select gp_inject_fault('analyze_finished_one_relation', 'skip', '', '', 'roottab', 1, -1, 0, 1);
alter table rootTabMid1 detach partition rootTabLeaf2;
alter system set autovacuum_naptime = 2;
select * from pg_reload_conf();
select gp_wait_until_triggered_fault('analyze_finished_one_relation', 1, 1);
select gp_inject_fault('analyze_finished_one_relation', 'reset', 1);
alter system set autovacuum_naptime = 2147483;
select * from pg_reload_conf();
select count(*) from roottab;
select tablename, attname,inherited,histogram_bounds from pg_stats where tablename like 'root%' order by tablename;

-----------------------------------------------
-- Case 4 - If the table is not analyzed, but all the existing leafs are
--     analyzed, then if an 'un-Analyzed Partition' is attached/detached,
--     merging of leaf stats is not expected. Note that the attached/detached,
--     partition is also not auto-analyzed.
--
--         Root  Not Analyzed
--         Leaf1 Analyzed
--         Leaf2 Not Analyzed
-----------------------------------------------
-- 1. Prepare basic framework
drop table if exists rootTab;
drop table if exists rootTabLeaf1;
drop table if exists rootTabLeaf2;
create table rootTab(a int)  partition by range(a);
create table rootTabMid1(a int) partition by range(a);
alter table rootTab attach partition rootTabMid1 for values from (0) to (20);
create table rootTabLeaf1(a int);
insert into rootTabLeaf1 select i%10 from generate_series(0,4)i;
analyze rootTabLeaf1;
select tablename, attname,inherited,histogram_bounds from pg_stats where tablename like 'root%' order by tablename;

-- 2. Attach analyed partition(Leaf1), wait for autoanalyze to trigger and check that root stats are updated.
select gp_inject_fault('analyze_finished_one_relation', 'skip', '', '', 'roottab', 1, -1, 0, 1);
alter table rootTabMid1 attach partition rootTabLeaf1 for values from (0) to (10);
alter system set autovacuum_naptime = 2;
select * from pg_reload_conf();
select gp_wait_until_triggered_fault('analyze_finished_one_relation', 1, 1);
select gp_inject_fault('analyze_finished_one_relation', 'reset', 1);
alter system set autovacuum_naptime = 2147483;
select * from pg_reload_conf();
select count(*) from roottab;
select tablename, attname,inherited,histogram_bounds from pg_stats where tablename like 'root%' order by tablename;

-- 3. Create new partition (Leaf2) to be added
create table rootTabLeaf2(a int);
insert into rootTabLeaf2 select i%20 from generate_series(10,19)i;

-- 4. Update rootTabLeaf1 to check, if it is analyzed when a new partiton is attached.
-- These should not be present in the root stats, after attach/detach of partition
insert into rootTabLeaf1 select i%10 from generate_series(5,9)i;

-- 5. Attach new partition, wait for autoanalyze to trigger and check that root stats are not updated
-- with leaf2 data, as leaf2 was not analyzed. Note that auto analyze did trigger after attach command.
select gp_inject_fault('analyze_finished_one_relation', 'skip', '', '', 'roottab', 1, -1, 0, 1);
alter table rootTabMid1 attach partition rootTabLeaf2 for values from (10) to (20);
alter system set autovacuum_naptime = 2;
select * from pg_reload_conf();
select gp_wait_until_triggered_fault('analyze_finished_one_relation', 1, 1);
select gp_inject_fault('analyze_finished_one_relation', 'reset', 1);
alter system set autovacuum_naptime = 2147483;
select * from pg_reload_conf();
select count(*) from roottab;
select tablename, attname,inherited,histogram_bounds from pg_stats where tablename like 'root%' order by tablename;

-- 6. Detach Partition (Leaf2) and check that root stats are not updated.
select gp_inject_fault('analyze_finished_one_relation', 'skip', '', '', 'roottab', 1, -1, 0, 1);
alter table rootTabMid1 detach partition rootTabLeaf2;
alter system set autovacuum_naptime = 2;
select * from pg_reload_conf();
select gp_wait_until_triggered_fault('analyze_finished_one_relation', 1, 1);
select gp_inject_fault('analyze_finished_one_relation', 'reset', 1);
alter system set autovacuum_naptime = 2147483;
select * from pg_reload_conf();
select count(*) from roottab;
select tablename, attname,inherited,histogram_bounds from pg_stats where tablename like 'root%' order by tablename;

-----------------------------------------------
-- Case 5 - If the table is not analyzed, then if an 'Analyzed Partition'
--     is attached/detached, merging of leaf stats is not expected
--
--         Root  Not Analyzed
--         Leaf1 Not Analyzed
--         Leaf2 Analyzed
-----------------------------------------------
-- 1. Prepare basic framework
drop table if exists rootTab;
drop table if exists rootTabLeaf1;
drop table if exists rootTabLeaf2;
create table rootTab(a int)  partition by range(a);
create table rootTabMid1(a int) partition by range(a);
alter table rootTab attach partition rootTabMid1 for values from (0) to (20);
create table rootTabLeaf1(a int);
insert into rootTabLeaf1 select i%10 from generate_series(0,4)i;
alter table rootTabMid1 attach partition rootTabLeaf1 for values from (0) to (10);

-- 2. Analyzing the new partition (Leaf2) to be added
create table rootTabLeaf2(a int);
insert into rootTabLeaf2 select i%20 from generate_series(10,19)i;
analyze rootTabLeaf2;

-- 3. Update rootTabLeaf1 to check, if it is analyzed when a new partiton is attached.
-- These should not be present in the root stats, after attach/detach of partition
insert into rootTabLeaf1 select i%10 from generate_series(5,9)i;

-- 4. Attach new partition, wait for autoanalyze to trigger and check that root stats are not updated
select gp_inject_fault('analyze_finished_one_relation', 'skip', '', '', 'roottab', 1, -1, 0, 1);
alter table rootTabMid1 attach partition rootTabLeaf2 for values from (10) to (20);
alter system set autovacuum_naptime = 2;
select * from pg_reload_conf();
select gp_wait_until_triggered_fault('analyze_finished_one_relation', 1, 1);
select gp_inject_fault('analyze_finished_one_relation', 'reset', 1);
alter system set autovacuum_naptime = 2147483;
select * from pg_reload_conf();
select count(*) from roottab;
select tablename, attname,inherited,histogram_bounds from pg_stats where tablename like 'root%' order by tablename;

-- 5. Detach Partition (Leaf2) and check that root stats are not updated.
select gp_inject_fault('analyze_finished_one_relation', 'skip', '', '', 'roottab', 1, -1, 0, 1);
alter table rootTabMid1 detach partition rootTabLeaf2;
alter system set autovacuum_naptime = 2;
select * from pg_reload_conf();
select gp_wait_until_triggered_fault('analyze_finished_one_relation', 1, 1);
select gp_inject_fault('analyze_finished_one_relation', 'reset', 1);
alter system set autovacuum_naptime = 2147483;
select * from pg_reload_conf();
select count(*) from roottab;
select tablename, attname,inherited,histogram_bounds from pg_stats where tablename like 'root%' order by tablename;


-----------------------------------------------
-- Case 6 - If the table is not analyzed, then if an 'Un-Analyzed Partition'
--     is attached/detached, merging of leaf stats is not expected.
--     Note that the attached/detached, partition is also not auto-analyzed.
--
--         Root  Not Analyzed
--         Leaf1 Not Analyzed
--         Leaf2 Not Analyzed
-----------------------------------------------
-- 1. Prepare basic framework
drop table if exists rootTab;
drop table if exists rootTabLeaf1;
drop table if exists rootTabLeaf2;
create table rootTab(a int)  partition by range(a);
create table rootTabMid1(a int) partition by range(a);
alter table rootTab attach partition rootTabMid1 for values from (0) to (20);
create table rootTabLeaf1(a int);
insert into rootTabLeaf1 select i%10 from generate_series(0,4)i;
alter table rootTabMid1 attach partition rootTabLeaf1 for values from (0) to (10);

-- 2. Creating the new partition(Leaf2) to be added
create table rootTabLeaf2(a int);
insert into rootTabLeaf2 select i%20 from generate_series(10,19)i;

-- 3. Update rootTabLeaf1 to check, if it is analyzed when a new partiton is attached.
-- These should not be present in the root stats, after attach/detach of partition
insert into rootTabLeaf1 select i%10 from generate_series(5,9)i;

-- 4. Attach new partition, wait for autoanalyze to trigger and check that root stats are not updated
select gp_inject_fault('analyze_finished_one_relation', 'skip', '', '', 'roottab', 1, -1, 0, 1);
alter table rootTabMid1 attach partition rootTabLeaf2 for values from (10) to (20);
alter system set autovacuum_naptime = 2;
select * from pg_reload_conf();
select gp_wait_until_triggered_fault('analyze_finished_one_relation', 1, 1);
select gp_inject_fault('analyze_finished_one_relation', 'reset', 1);
alter system set autovacuum_naptime = 2147483;
select * from pg_reload_conf();
select count(*) from roottab;
select tablename, attname,inherited,histogram_bounds from pg_stats where tablename like 'root%' order by tablename;

-- 5. Detach Partition (Leaf2), wait for autoanalyze to trigger and check that root stats are not updated
select gp_inject_fault('analyze_finished_one_relation', 'skip', '', '', 'roottab', 1, -1, 0, 1);
alter table rootTabMid1 detach partition rootTabLeaf2;
alter system set autovacuum_naptime = 2;
select * from pg_reload_conf();
select gp_wait_until_triggered_fault('analyze_finished_one_relation', 1, 1);
select gp_inject_fault('analyze_finished_one_relation', 'reset', 1);
alter system set autovacuum_naptime = 2147483;
select * from pg_reload_conf();
select count(*) from roottab;
select tablename, attname,inherited,histogram_bounds from pg_stats where tablename like 'root%' order by tablename;

-----------------------------------------------
-- Case 7 - If a single leaf is attached/detached to/from the root.
--          Root stats are updated when leaf is attached,
--          but not when detached as only root is left.
--
--          Structure - RootTab -->  Leaf1
--          Root  Analyzed
--          Leaf1 Analyzed
-----------------------------------------------
--1. Create root table
drop table if exists rootTab;
drop table if exists rootTabLeaf1;
drop table if exists rootTabLeaf2;
create table rootTab(a int)  partition by range(a);
analyze rootTab;

--2. Create leaf table
create table rootTableaf1(a int);
insert into rootTableaf1 select i%10 from generate_series(0,7)i;
analyze rootTableaf1;
select tablename, attname,inherited,histogram_bounds from pg_stats where tablename like 'root%' order by tablename;

--3. Attach leaf, wait for autoanalyze to trigger and check that root stats are updated
select gp_inject_fault('analyze_finished_one_relation', 'skip', '', '', 'roottab', 1, -1, 0, 1);
alter table rootTab attach partition rootTableaf1 for values from (0) to (10);
alter system set autovacuum_naptime = 2;
select * from pg_reload_conf();
select gp_wait_until_triggered_fault('analyze_finished_one_relation', 1, 1);
select gp_inject_fault('analyze_finished_one_relation', 'reset', 1);
alter system set autovacuum_naptime = 2147483;
select * from pg_reload_conf();
select count(*) from roottab;
select tablename, attname,inherited,histogram_bounds from pg_stats where tablename like 'root%' order by tablename;

--4. Detach leaf and check that root stats are not updated.
--   After Detach, only root is left, so Analyze is not performed on it.
select gp_inject_fault('analyze_finished_one_relation', 'skip', '', '', 'roottab', 1, -1, 0, 1);
alter table rootTab detach partition rootTabLeaf1;
select * from rootTab;
alter system set autovacuum_naptime = 2;
select * from pg_reload_conf();
select gp_wait_until_triggered_fault('analyze_finished_one_relation', 1, 1);
select gp_inject_fault('analyze_finished_one_relation', 'reset', 1);
alter system set autovacuum_naptime = 2147483;
select * from pg_reload_conf();
select count(*) from roottab;
select tablename, attname,inherited,histogram_bounds from pg_stats where tablename like 'root%' order by tablename;

-----------------------------------------------
-- Case 8 - If root table, middle table are left after leaf is detached
--          Root stats should be updated when leaf is attached, but not when detached
--
--          Structure - RootTab --> Mid --> Leaf1
--          Root  Analyzed
--          Leaf1 Analyzed
-----------------------------------------------
--1. Create root and middle table
drop table if exists rootTab;
drop table if exists rootTabLeaf1;
drop table if exists rootTabLeaf2;
create table rootTab(a int)  partition by range(a);
create table rootTabMid1(a int) partition by range(a);
alter table rootTab attach partition rootTabMid1 for values from (0) to (20);
analyze rootTab;

--2. Create leaf table
create table rootTableaf1(a int);
insert into rootTableaf1 select i%10 from generate_series(0,7)i;
analyze rootTableaf1;
select tablename, attname,inherited,histogram_bounds from pg_stats where tablename like 'root%' order by tablename;

--3. Attach leaf, wait for autoanalyze to trigger and check that root stats are updated
select gp_inject_fault('analyze_finished_one_relation', 'skip', '', '', 'roottab', 1, -1, 0, 1);
alter table rootTabMid1 attach partition rootTabLeaf1 for values from (0) to (10);
alter system set autovacuum_naptime = 2;
select * from pg_reload_conf();
select gp_wait_until_triggered_fault('analyze_finished_one_relation', 1, 1);
select gp_inject_fault('analyze_finished_one_relation', 'reset', 1);
alter system set autovacuum_naptime = 2147483;
select * from pg_reload_conf();
select count(*) from roottab;
select tablename, attname,inherited,histogram_bounds from pg_stats where tablename like 'root%' order by tablename;

--4. Detach leaf and check that root stats are not updated.
--   After Detach, no leaf is present, so Analyze is not performed.
select gp_inject_fault('analyze_finished_one_relation', 'skip', '', '', 'roottab', 1, -1, 0, 1);
alter table rootTabMid1 detach partition rootTabLeaf1;
alter system set autovacuum_naptime = 2;
select * from pg_reload_conf();
select gp_wait_until_triggered_fault('analyze_finished_one_relation', 1, 1);
select gp_inject_fault('analyze_finished_one_relation', 'reset', 1);
alter system set autovacuum_naptime = 2147483;
select * from pg_reload_conf();
select count(*) from roottab;
select tablename, attname,inherited,histogram_bounds from pg_stats where tablename like 'root%' order by tablename;

-- clean up in the end
set optimizer_trace_fallback to off;
drop table if exists rootTab;
drop table if exists rootTabMid1;
drop table if exists rootTabLeaf1;
drop table if exists rootTabLeaf2;
ALTER SYSTEM RESET autovacuum_naptime;
ALTER SYSTEM RESET autovacuum_vacuum_threshold;
ALTER SYSTEM RESET autovacuum_analyze_threshold;
select * from pg_reload_conf();
