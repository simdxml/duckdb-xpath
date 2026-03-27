-- Benchmark: duckdb-xpath (simdxml)
-- Usage: duckdb -unsigned -no-stdin < test/benchmark/bench_simdxml.sql

.timer on

-- Setup
CREATE OR REPLACE TABLE xml_docs AS
SELECT
    CASE WHEN i % 10 = 0
        THEN '<patent><title>Widget ' || i || '</title><claim type="independent">A device comprising component ' || i || '</claim><claim type="dependent">The device of claim 1 wherein ' || i || '</claim></patent>'
        ELSE '<patent><title>Widget ' || i || '</title><abstract>An abstract about widget ' || i || ' with many details and specifications that make the document larger</abstract><description>Detailed description of widget ' || i || '</description></patent>'
    END AS xml,
    i
FROM generate_series(1, 100000) t(i);

LOAD 'build/debug/extension/duckdb_xpath/duckdb_xpath.duckdb_extension';

SELECT '=== duckdb-xpath (simdxml) ===';

SELECT printf('%.1f MB total XML', sum(length(xml)) / 1e6) FROM xml_docs;

-- B1: xpath_text //claim (10% match)
SELECT '--- B1: xpath_text //claim (10% match) ---';
SELECT count(*) FROM (
    SELECT xpath_text(xml, '//claim') AS r FROM xml_docs
) WHERE r != '';

-- B2: xpath_text //title (100% match)
SELECT '--- B2: xpath_text //title (100% match) ---';
SELECT count(*) FROM (
    SELECT xpath_text(xml, '//title') AS r FROM xml_docs
) WHERE r != '';

-- B3: xpath_exists //claim (10% match)
SELECT '--- B3: xpath_exists //claim (10% match) ---';
SELECT count(*) FROM xml_docs WHERE xpath_exists(xml, '//claim');

-- B4: xpath_count //claim
SELECT '--- B4: xpath_count //claim ---';
SELECT sum(xpath_count(xml, '//claim')) AS total_claims FROM xml_docs;

-- B5: xpath_text with predicate (attribute filter)
SELECT '--- B5: xpath_text //claim[@type="independent"] ---';
SELECT count(*) FROM (
    SELECT xpath_text(xml, '//claim[@type="independent"]') AS r FROM xml_docs
) WHERE r != '';

-- B6: xpath_list //claim (JSON array, 10% match)
SELECT '--- B6: xpath_list //claim ---';
SELECT count(*) FROM (
    SELECT xpath_list(xml, '//claim') AS r FROM xml_docs
) WHERE r != '[]';
