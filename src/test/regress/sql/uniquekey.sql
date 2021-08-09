CREATE TABLE uqk1(a int, pk int primary key, c int,  d int);
CREATE TABLE uqk2(a int, pk int primary key, c int,  d int);
INSERT INTO uqk1 VALUES(1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3), (4, 4, null, 4), (5, 5, null, 4);
INSERT INTO uqk2 VALUES(1, 1, 1, 1), (4, 4, 4, 4), (5, 5, 5, 5);
ANALYZE uqk1;
ANALYZE uqk2;

-- Test single table primary key.
EXPLAIN (COSTS OFF) SELECT DISTINCT * FROM uqk1;

-- Test EC case.
EXPLAIN (COSTS OFF) SELECT DISTINCT d FROM uqk1 WHERE d = pk;

-- Test UniqueKey indexes.
CREATE UNIQUE INDEX uqk1_ukcd ON uqk1(c, d);

-- Test not null quals and not null per catalog.
EXPLAIN (COSTS OFF) SELECT DISTINCT c, d FROM uqk1;
EXPLAIN (COSTS OFF) SELECT DISTINCT c, d FROM uqk1 WHERE c is NOT NULL;
EXPLAIN (COSTS OFF) SELECT DISTINCT d FROM uqk1 WHERE c = 1;
ALTER TABLE uqk1 ALTER COLUMN d SET NOT NULL;
EXPLAIN (COSTS OFF) SELECT DISTINCT c, d FROM uqk1 WHERE c is NOT NULL;

-- Test UniqueKey column reduction.
EXPLAIN (COSTS OFF) SELECT DISTINCT d FROM uqk1 WHERE c = 1;
EXPLAIN (COSTS OFF) SELECT DISTINCT a FROM uqk1 WHERE c = 1 and d = 1;

-- Test Distinct ON
EXPLAIN (COSTS OFF) SELECT DISTINCT ON(pk) d FROM uqk1;
