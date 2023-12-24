create table t1(a text, b text, c text);
create table t2(a text, b text, c text);
create table t3(a text, b text, c text);

insert into t1 select i, i, i from generate_series(1, 1000000)i;
insert into t2 select i, i, i from generate_series(1, 1000000)i;
insert into t3 select i, i, i from generate_series(1, 1000000)i;

create index on t1(c);

analyze t1;
analyze t2;
analyze t3;

-- Turn off jit first, reasons:
-- 1. JIT is not adapted for this feature, it may cause crash on jit.
-- 2. more logging for this feature is enabled when jit=off
set jit to off;

explain (verbose) select * from t1 where b > 'a';


-- NullTest has nothing with tts_values, so its access to toast value
-- should be ignored.
explain (verbose) select * from t1 where b is NULL and c is not null;

-- b can't be shared-detoasted since it would make the work_mem bigger.
explain (verbose) select * from t1 where b > 'a' order by c;

-- b CAN be shared-detoasted since it would NOT make the work_mem bigger.
-- but compared with the old behavior, it cause the lifespan of the 'detoast datum'
-- longer, in the old behavior, it is reset becase of ExecQualAndReset.
explain (verbose) select a, c from t1 where b > 'a' order by c;

-- The detoast only happen at the join stage.
explain (verbose) select * from t1 join t2 using(b);

--
explain (verbose) select * from t1 join t2 using(b) where t1.c > '3';

explain (verbose)
select t3.*
from t1, t2, t3
where t2.c > '999999999999999'
and t2.c = t1.c
and t3.b = t1.b;
