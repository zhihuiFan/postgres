set geqo to off;
create table t1(a int, b int not null, c int, d int);
create table t2(a int, b int not null, c int, d int);

-- single rel
select * from t1;
select * from t1 where a > 1;
select * from t2 where a > 1 or c > 1;

-- partitioned relation.
-- append rel: all the childrel are not nullable.
create table p (a int, b int, c int not null) partition by range(a);
create table p_1 partition of p for values from (0) to (10000) partition by list(b);
create table p_1_1(b int,  c int not null, a int);
alter table p_1 attach partition p_1_1 for values in (1);
create table p_2 partition of p for values from (10001) to (20000);

-- p(1)  - 3(c)
-- p_1(2) - 3(c)
-- p_1_1(3) - 2(c)
select * from p;
-- p(1)  - 3(c) 1(a)
-- p_1(2) - 3(c) 1(a)
-- p_1_1(3) - 2(c) 3(a)
select * from p where a > 1;

-- test join:
-- t1: b
-- t2: b
-- t{1, 2}:  t1.a, t1.b t2.b t2.c
select * from t1, t2 where t1.a = t2.c;

-- t1: b
-- t2: b
-- t{1, 2}:  none due to full join
select * from t1 full join t2 on t1.a = t2.a;

-- t1: b
-- t2: b
-- t{1, 2}:  t1.b t1.a   (t2.a t2.b is nullable due to outer join)
select * from t1 left join t2 on t1.a = t2.a;


-- test upper rel.
select * from t1 order by a;
select * from t1 where a > 1 order by a;
select * from t2 where a > 1 or c > 1 order by a;

select a, count(*) from t1 group by a;
select a, count(*) from t1 where a > 1 group by a;
select a, count(*) from t2 where a > 1 or c > 1 group by a;

select DISTINCT * from t1 order by a;
select DISTINCT * from t1 where a > 1 order by a;
select DISTINCT * from t2 where a > 1 or c > 1 order by a;

select * from t1 left join t2 on t1.a = t2.a order by t2.a;

select * from t1 join t2 on t1.a = t2.a order by t2.a;

--  subquery

select * from
(select t1.a as t1a, t1.b as t1b, t1.c as t1c,
 t2.* from t1 join t2 on t1.a = t2.a order by t2.a limit 3) as x
where t1c > 3;


-- SetOp RelOptInfo.


-- simple union all

select * from t1
union all
select * from t2;

select * from t1
union
select * from t2;




drop table t1;
drop table t2;
drop table p;
