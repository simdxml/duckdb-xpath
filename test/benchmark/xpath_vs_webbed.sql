-- duckdb-xpath vs webbed benchmark
-- Run separately for each extension to avoid function name conflicts.
--
-- Usage:
--   # duckdb-xpath (simdxml):
--   duckdb -unsigned -c ".read test/benchmark/setup.sql" -c "LOAD 'build/debug/extension/duckdb_xpath/duckdb_xpath.duckdb_extension';" -c ".read test/benchmark/queries.sql"
--
--   # webbed (libxml2):
--   duckdb -unsigned -c ".read test/benchmark/setup.sql" -c "INSTALL webbed FROM community; LOAD webbed;" -c ".read test/benchmark/queries_webbed.sql"
--
-- Or just run this file for a quick side-by-side (loads duckdb-xpath first, webbed second).

.timer on

-- =========================================================================
-- Setup: generate 100K XML documents
-- =========================================================================

CREATE OR REPLACE TABLE xml_docs AS
SELECT
    CASE WHEN i % 10 = 0
        THEN '<patent><title>Widget ' || i || '</title><claim type="independent">A device comprising component ' || i || '</claim><claim type="dependent">The device of claim 1 wherein ' || i || '</claim></patent>'
        ELSE '<patent><title>Widget ' || i || '</title><abstract>An abstract about widget ' || i || ' with many details and specifications that make the document larger</abstract><description>Detailed description of widget ' || i || '</description></patent>'
    END AS xml,
    i
FROM generate_series(1, 100000) t(i);

SELECT count(*) AS total_docs,
       count(*) FILTER (WHERE xml LIKE '%<claim%') AS docs_with_claims,
       printf('%.1f MB', sum(length(xml)) / 1e6) AS total_size
FROM xml_docs;
