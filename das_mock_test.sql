DROP SCHEMA test CASCADE;
DROP SERVER mock_das CASCADE;
DROP EXTENSION multicorn CASCADE;
CREATE EXTENSION multicorn;
CREATE SERVER mock_das FOREIGN DATA WRAPPER multicorn OPTIONS (
    wrapper 'multicorn_das.DASFdw',
    das_url 'localhost:50051',
    das_type 'mock'
);                 
CREATE SCHEMA test;                                  
IMPORT FOREIGN SCHEMA foo FROM SERVER mock_das INTO test;
SELECT * FROM test.small;
SELECT * FROM test.small LIMIT 10;
SELECT * FROM test.all_types LIMIT 10;
SELECT * FROM test.in_memory;
INSERT INTO test.in_memory VALUES (1, 'One');
INSERT INTO test.in_memory VALUES (2, 'Two');
UPDATE test.in_memory SET column2 = 'Um' WHERE column1 = 1;
SELECT * FROM test.in_memory WHERE column2 = 'Um';
DELETE FROM test.in_memory WHERE column1 = 2;
SELECT * FROM test.in_memory;
DELETE FROM test.in_memory;
SELECT COUNT(*) FROM test.in_memory;
DROP SCHEMA test CASCADE;
DROP SERVER mock_das CASCADE;
