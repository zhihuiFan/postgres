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

-- TODO: union, simple union all, subquery, group by, order by, WindowAgg.

drop table t1;
drop table t2;
drop table p;
