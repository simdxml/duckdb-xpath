# duckdb-xpath

DuckDB extension for XPath queries on XML columns. Uses [simdxml](https://github.com/simdxml/simdxml) for parsing.

## Functions

| Function | Return | Description |
|---|---|---|
| `xpath_text(xml, xpath)` | `VARCHAR` | Text of first matching node |
| `xpath_list(xml, xpath)` | `VARCHAR` | JSON array of all matching text |
| `xpath_count(xml, xpath)` | `BIGINT` | Count of matching nodes |
| `xpath_exists(xml, xpath)` | `BOOLEAN` | Whether any node matches |
| `xpath_eval(xml, xpath)` | `VARCHAR` | Scalar XPath expressions (count(), string(), etc.) |
| `read_xml(file, xpath)` | Table | Each matching text as a row |
| `xpath_create_index(file)` | Table | Build `.sxi` sidecar index |
| `xpath_drop_index(file)` | Table | Remove `.sxi` sidecar index |

```sql
SELECT xpath_text(xml, '//invention-title') FROM patents;
SELECT ucid, xpath_count(xml, '//claim-text') AS n_claims FROM patents;
SELECT * FROM read_xml('corpus.xml', '//patent/title');
```

## Benchmark

10K real patent XML documents (1.1 GB, 111 KB avg), release build, compared to [webbed](https://duckdb.org/community_extensions/extensions/webbed) (libxml2):

| Query | duckdb-xpath | webbed | Ratio |
|---|---|---|---|
| `//invention-title` text (100% match) | 0.005s | 3.42s | 685x |
| `xpath_exists //claim-text` (93% match) | 0.025s | 3.71s | 148x |
| `xpath_count //claim-text` (461K total) | 0.035s | 3.64s | 104x |
| `xpath_text //claim-text` (93% match) | 0.655s | 3.75s | 5.7x |

Simple `//tagname` queries use a grep-mode fast path (SIMD string search, no XML parsing). Complex queries fall through to structural parsing with buffer reuse across documents.

Corpus files are in `test/benchmark/` (git-lfs).

## Building

```bash
git clone --recurse-submodules <repo>
cd duckdb-xpath
make configure
make release
make test_release
```

Extension output: `build/release/extension/duckdb_xpath/duckdb_xpath.duckdb_extension`

```sql
LOAD '/path/to/duckdb_xpath.duckdb_extension';
```

## XPath coverage

XPath 1.0: 327/327 libxml2 conformance, 1015/1023 pugixml conformance. XPath 2.0/3.1 in progress upstream in simdxml.

## License

MIT OR Apache-2.0
