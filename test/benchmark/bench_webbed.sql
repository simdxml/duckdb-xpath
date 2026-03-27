-- Benchmark: webbed (libxml2)
-- Usage: duckdb -no-stdin < test/benchmark/bench_webbed.sql

.timer on

-- Setup (identical data)
CREATE OR REPLACE TABLE xml_docs AS
SELECT
    CASE WHEN i % 10 = 0
        THEN '<patent><title>Widget ' || i || '</title><claim type="independent">A device comprising component ' || i || '</claim><claim type="dependent">The device of claim 1 wherein ' || i || '</claim></patent>'
        ELSE '<patent><title>Widget ' || i || '</title><abstract>An abstract about widget ' || i || ' with many details and specifications that make the document larger</abstract><description>Detailed description of widget ' || i || '</description></patent>'
    END AS xml,
    i
FROM generate_series(1, 100000) t(i);

INSTALL webbed FROM community;
LOAD webbed;

SELECT '=== webbed (libxml2) ===';

SELECT printf('%.1f MB total XML', sum(length(xml)) / 1e6) FROM xml_docs;

-- B1: xpath_text //claim (10% match)
SELECT '--- B1: xml_extract_text //claim (10% match) ---';
SELECT count(*) FROM (
    SELECT xml_extract_text(xml, '//claim') AS r FROM xml_docs
) WHERE len(r) > 0;

-- B2: xpath_text //title (100% match)
SELECT '--- B2: xml_extract_text //title (100% match) ---';
SELECT count(*) FROM (
    SELECT xml_extract_text(xml, '//title') AS r FROM xml_docs
) WHERE len(r) > 0;

-- B3: xpath_exists //claim (10% match) — webbed has xml_exists
SELECT '--- B3: xml_exists //claim (10% match) ---';
SELECT count(*) FROM xml_docs WHERE xml_exists(xml, '//claim');

-- B4: xpath_count //claim — use len(xml_extract_text)
SELECT '--- B4: count via len(xml_extract_text //claim) ---';
SELECT sum(len(xml_extract_text(xml, '//claim'))) AS total_claims FROM xml_docs;

-- B5: xpath_text with predicate (attribute filter)
SELECT '--- B5: xml_extract_text //claim[@type="independent"] ---';
SELECT count(*) FROM (
    SELECT xml_extract_text(xml, '//claim[@type="independent"]') AS r FROM xml_docs
) WHERE len(r) > 0;

-- B6: xpath_list — webbed already returns list, same as xml_extract_text
SELECT '--- B6: xml_extract_text //claim (list) ---';
SELECT count(*) FROM (
    SELECT xml_extract_text(xml, '//claim') AS r FROM xml_docs
) WHERE len(r) > 0;
