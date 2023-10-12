CREATE TABLE uk_t (id int primary key, a int not null, b int not null, c int, d int, e int);

explain (costs off)
select distinct id from uk_t;

explain (costs off)
select distinct e from uk_t where id = e;

create unique index on uk_t (a, b);
create unique index on uk_t (c, d);

explain (costs off)
select distinct a, b from uk_t;

explain (costs off)
select distinct c, d from uk_t;

explain (costs off)
select distinct c, d from uk_t
where c > 0 and d > 0;

explain (costs off)
select distinct d from uk_t
where c > 1 and d > 0;

explain (costs off)
select distinct d from uk_t
where c = 1 and d > 0;





