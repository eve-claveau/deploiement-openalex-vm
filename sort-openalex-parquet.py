"""
sort-openalex-parquet.py

Post-processing step for flatten-openalex-parquet.py.

Reads each Parquet file produced by the flattener, sorts it by the column(s)
most likely to be used in filters and joins, then rewrites it in place with
tuned row-group sizes.

Why this helps in DuckDB
------------------------
Parquet stores min/max statistics for every column in every row group.  When
DuckDB evaluates a WHERE clause or a JOIN condition it checks those stats first
and skips entire row groups that cannot contain matching values.  This only
works when the data is actually sorted — on random data every row group's range
spans the whole domain and nothing gets skipped.

Sorting is the Parquet-native equivalent of a clustered (covering) index in
Postgres.  It won't beat a B-tree for single-row point lookups, but for the
analytical join/filter patterns typical of OpenAlex queries it closes most of
the gap.

Row-group size guidance
-----------------------
Smaller row groups  → finer-grained skipping, more metadata overhead.
Larger row groups   → better compression, less metadata, coarser skipping.
The defaults below are a reasonable starting point:
  - Large tables (works*, authors*): 100 000 rows
  - Small/medium tables:             50 000 rows
Override with OPENALEX_ROW_GROUP_SIZE env-var.

Usage
-----
    # Sort all tables in place (default parquet-files/ dir)
    python sort-openalex-parquet.py

    # Sort into a NEW directory, leaving the source untouched (e.g. for testing)
    PARQUET_DIR=/path/to/parquets OUTPUT_DIR=/path/to/parquets-sorted \\
        python sort-openalex-parquet.py

    # Process only one table (useful for re-sorting after partial updates)
    OPENALEX_ONLY=works_authorships python sort-openalex-parquet.py

    # Tune row-group size
    OPENALEX_ROW_GROUP_SIZE=200000 python sort-openalex-parquet.py

Dependencies
------------
    pip install pyarrow
    (no DuckDB needed for this script itself)
"""

import os
import tempfile
import shutil

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PARQUET_DIR       = os.environ.get('PARQUET_DIR', 'parquet-files')
# If set, sorted files are written here instead of overwritten in place.
# Lets you test the sort on a fresh copy without touching your existing files.
# If unset, defaults to PARQUET_DIR (original in-place behaviour).
OUTPUT_DIR        = os.environ.get('OUTPUT_DIR', '') or PARQUET_DIR
ONLY_TABLE        = os.environ.get('OPENALEX_ONLY', '')          # optional filter
DEFAULT_RG_SIZE   = int(os.environ.get('OPENALEX_ROW_GROUP_SIZE', '100000'))

# ---------------------------------------------------------------------------
# Sort plan
#
# Each entry:
#   table_name  : matches the .parquet filename (without extension)
#   sort_keys   : list of (column, order) tuples passed to Table.sort_by()
#                 First key is primary; subsequent keys break ties.
#   row_group   : row-group size — smaller = finer skip granularity
#
# Rationale per table
# -------------------
# works                         → id         primary lookup / join target
# works_authorships             → work_id    largest child table; also sorted
#                                  author_id  secondarily so author-centric
#                                             queries skip efficiently too
# works_locations               → work_id    joined to works on every OA query
# works_primary_locations       → work_id    same
# works_best_oa_locations       → work_id    same
# works_concepts / topics       → work_id    topic/concept distribution queries
# works_ids                     → work_id    DOI / PMID lookups go through here
# works_referenced_works        → work_id    citation graph traversals
# works_related_works           → work_id    recommendation queries
# works_open_access             → work_id    OA-status filters are very common
# works_biblio                  → work_id    joined when volume/issue needed
# works_mesh                    → work_id    biomedical queries
# authors                       → id         same reasoning as works
# authors_ids                   → author_id
# authors_counts_by_year        → author_id, year  time-series per author
# concepts                      → id
# concepts_ancestors            → concept_id
# concepts_counts_by_year       → concept_id, year
# concepts_related_concepts     → concept_id
# concepts_ids                  → concept_id
# institutions                  → id
# institutions_ids              → institution_id
# institutions_geo              → institution_id
# institutions_associated_*     → institution_id
# institutions_counts_by_year   → institution_id, year
# publishers                    → id
# publishers_ids                → publisher_id
# publishers_counts_by_year     → publisher_id, year
# sources                       → id
# sources_ids                   → source_id
# sources_counts_by_year        → source_id, year
# topics                        → id
# ---------------------------------------------------------------------------

SORT_PLAN = [
    # ---- works (largest entity — prioritise) --------------------------------
    {
        'table':      'works',
        'sort_keys':  [('id', 'ascending')],
        'row_group':  100_000,
    },
    {
        'table':      'works_authorships',
        # Primary sort by work_id keeps work-centric joins fast.
        # Secondary sort by author_id means that within each work the author
        # rows are ordered, which also helps author-centric range scans
        # (though not as much as a dedicated author_id-sorted copy would).
        'sort_keys':  [('work_id', 'ascending'), ('author_id', 'ascending')],
        'row_group':  100_000,
    },
    {
        'table':      'works_locations',
        'sort_keys':  [('work_id', 'ascending')],
        'row_group':  100_000,
    },
    {
        'table':      'works_primary_locations',
        'sort_keys':  [('work_id', 'ascending')],
        'row_group':  100_000,
    },
    {
        'table':      'works_best_oa_locations',
        'sort_keys':  [('work_id', 'ascending')],
        'row_group':  100_000,
    },
    {
        'table':      'works_concepts',
        'sort_keys':  [('work_id', 'ascending')],
        'row_group':  100_000,
    },
    {
        'table':      'works_topics',
        'sort_keys':  [('work_id', 'ascending')],
        'row_group':  100_000,
    },
    {
        'table':      'works_ids',
        'sort_keys':  [('work_id', 'ascending')],
        'row_group':  100_000,
    },
    {
        'table':      'works_referenced_works',
        'sort_keys':  [('work_id', 'ascending')],
        'row_group':  100_000,
    },
    {
        'table':      'works_related_works',
        'sort_keys':  [('work_id', 'ascending')],
        'row_group':  100_000,
    },
    {
        'table':      'works_open_access',
        'sort_keys':  [('work_id', 'ascending')],
        'row_group':  100_000,
    },
    {
        'table':      'works_biblio',
        'sort_keys':  [('work_id', 'ascending')],
        'row_group':  100_000,
    },
    {
        'table':      'works_mesh',
        'sort_keys':  [('work_id', 'ascending')],
        'row_group':  100_000,
    },

    # ---- authors ------------------------------------------------------------
    {
        'table':      'authors',
        'sort_keys':  [('id', 'ascending')],
        'row_group':  100_000,
    },
    {
        'table':      'authors_ids',
        'sort_keys':  [('author_id', 'ascending')],
        'row_group':  50_000,
    },
    {
        'table':      'authors_counts_by_year',
        'sort_keys':  [('author_id', 'ascending'), ('year', 'ascending')],
        'row_group':  50_000,
    },

    # ---- concepts -----------------------------------------------------------
    {
        'table':      'concepts',
        'sort_keys':  [('id', 'ascending')],
        'row_group':  50_000,
    },
    {
        'table':      'concepts_ancestors',
        'sort_keys':  [('concept_id', 'ascending')],
        'row_group':  50_000,
    },
    {
        'table':      'concepts_counts_by_year',
        'sort_keys':  [('concept_id', 'ascending'), ('year', 'ascending')],
        'row_group':  50_000,
    },
    {
        'table':      'concepts_related_concepts',
        'sort_keys':  [('concept_id', 'ascending')],
        'row_group':  50_000,
    },
    {
        'table':      'concepts_ids',
        'sort_keys':  [('concept_id', 'ascending')],
        'row_group':  50_000,
    },

    # ---- institutions -------------------------------------------------------
    {
        'table':      'institutions',
        'sort_keys':  [('id', 'ascending')],
        'row_group':  50_000,
    },
    {
        'table':      'institutions_ids',
        'sort_keys':  [('institution_id', 'ascending')],
        'row_group':  50_000,
    },
    {
        'table':      'institutions_geo',
        'sort_keys':  [('institution_id', 'ascending')],
        'row_group':  50_000,
    },
    {
        'table':      'institutions_associated_institutions',
        'sort_keys':  [('institution_id', 'ascending')],
        'row_group':  50_000,
    },
    {
        'table':      'institutions_counts_by_year',
        'sort_keys':  [('institution_id', 'ascending'), ('year', 'ascending')],
        'row_group':  50_000,
    },

    # ---- publishers ---------------------------------------------------------
    {
        'table':      'publishers',
        'sort_keys':  [('id', 'ascending')],
        'row_group':  50_000,
    },
    {
        'table':      'publishers_ids',
        'sort_keys':  [('publisher_id', 'ascending')],
        'row_group':  50_000,
    },
    {
        'table':      'publishers_counts_by_year',
        'sort_keys':  [('publisher_id', 'ascending'), ('year', 'ascending')],
        'row_group':  50_000,
    },

    # ---- sources ------------------------------------------------------------
    {
        'table':      'sources',
        'sort_keys':  [('id', 'ascending')],
        'row_group':  50_000,
    },
    {
        'table':      'sources_ids',
        'sort_keys':  [('source_id', 'ascending')],
        'row_group':  50_000,
    },
    {
        'table':      'sources_counts_by_year',
        'sort_keys':  [('source_id', 'ascending'), ('year', 'ascending')],
        'row_group':  50_000,
    },

    # ---- topics -------------------------------------------------------------
    {
        'table':      'topics',
        'sort_keys':  [('id', 'ascending')],
        'row_group':  50_000,
    },
]


# ---------------------------------------------------------------------------
# Core sort-and-rewrite logic
# ---------------------------------------------------------------------------

def sort_table(entry: dict) -> None:
    table_name = entry['table']
    sort_keys  = entry['sort_keys']
    row_group  = entry.get('row_group', DEFAULT_RG_SIZE)

    src_path = os.path.join(PARQUET_DIR, f'{table_name}.parquet')
    dst_path = os.path.join(OUTPUT_DIR, f'{table_name}.parquet')
    in_place = os.path.abspath(OUTPUT_DIR) == os.path.abspath(PARQUET_DIR)

    if not os.path.exists(src_path):
        print(f'  [skip] {src_path} not found')
        return

    file_mb = os.path.getsize(src_path) / 1_048_576
    key_str  = ', '.join(k for k, _ in sort_keys)
    print(f'  {table_name:45s}  {file_mb:8.1f} MB  sort: [{key_str}]'
          f'{"  (in place)" if in_place else f"  -> {OUTPUT_DIR}"}')

    # Read the full table into memory.
    # For very large tables (works, works_authorships) this can be many GB.
    # If you run out of RAM, set OPENALEX_ROW_GROUP_SIZE lower and process
    # works-related tables on a machine with more memory, or stream-sort with
    # an external sort library.
    table = pq.read_table(src_path)

    # Sort
    table = table.sort_by(sort_keys)

    # Always write through a temp file then atomic-rename, whether writing
    # in place or to a separate output directory. This avoids ever leaving
    # a corrupt/partial file if the process is interrupted.
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    tmp_fd, tmp_path = tempfile.mkstemp(
        dir=OUTPUT_DIR, prefix=f'.{table_name}_tmp_', suffix='.parquet'
    )
    os.close(tmp_fd)

    try:
        pq.write_table(
            table,
            tmp_path,
            row_group_size=row_group,
            compression='snappy',
            write_statistics=True,
            # Bloom filters give DuckDB an additional way to skip row groups
            # for equality predicates (e.g. WHERE work_id = '...').
            # Enable on the primary sort column of each table.
            write_bloom_filter=True,
        )
        shutil.move(tmp_path, dst_path)
    except Exception:
        # Clean up temp file on failure
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise

    new_mb = os.path.getsize(dst_path) / 1_048_576
    print(f'    {"":45s}  {new_mb:8.1f} MB  (done)')


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    if not os.path.isdir(PARQUET_DIR):
        raise SystemExit(f'ERROR: PARQUET_DIR "{PARQUET_DIR}" does not exist. '
                         'Run flatten-openalex-parquet.py first.')

    plan = SORT_PLAN
    if ONLY_TABLE:
        plan = [e for e in plan if e['table'] == ONLY_TABLE]
        if not plan:
            raise SystemExit(f'ERROR: no entry for table "{ONLY_TABLE}" in sort plan.')

    in_place = os.path.abspath(OUTPUT_DIR) == os.path.abspath(PARQUET_DIR)
    print(f'Sorting {len(plan)} table(s)')
    print(f'  Source : {PARQUET_DIR}')
    print(f'  Output : {OUTPUT_DIR}' + (' (in place)' if in_place else ' (separate — source untouched)'))
    print()
    for entry in plan:
        sort_table(entry)

    print('\nDone.')
