"""
flatten-openalex-parquet.py

Converts OpenAlex JSONL snapshot files directly to Parquet format,
preserving the same relational table structure as the original
CSV-based pipeline (flatten-openalex-jsonl.py + openalex-pg-schema.sql).

Input layout (OpenAlex snapshot):
    data/
        authors/   updated_date=YYYY-MM-DD/*.gz
        concepts/  ...
        institutions/ ...
        publishers/ ...
        sources/   ...
        topics/    ...
        works/     ...
        (+ other entity dirs not modelled in the original schema)

Output layout:
    parquet-files/
        authors.parquet
        authors_ids.parquet
        authors_counts_by_year.parquet
        concepts.parquet
        concepts_ancestors.parquet
        concepts_counts_by_year.parquet
        concepts_ids.parquet
        concepts_related_concepts.parquet
        topics.parquet
        institutions.parquet
        institutions_ids.parquet
        institutions_geo.parquet
        institutions_associated_institutions.parquet
        institutions_counts_by_year.parquet
        publishers.parquet
        publishers_ids.parquet
        publishers_counts_by_year.parquet
        sources.parquet
        sources_ids.parquet
        sources_counts_by_year.parquet
        works.parquet
        works_primary_locations.parquet
        works_locations.parquet
        works_best_oa_locations.parquet
        works_authorships.parquet
        works_biblio.parquet
        works_topics.parquet
        works_concepts.parquet
        works_ids.parquet
        works_mesh.parquet
        works_open_access.parquet
        works_referenced_works.parquet
        works_related_works.parquet

Usage:
    # Full snapshot
    python flatten-openalex-parquet.py

    # Limit to N partition files per entity (useful for testing)
    OPENALEX_DEMO_FILES_PER_ENTITY=2 python flatten-openalex-parquet.py

    # Custom snapshot / output dirs
    SNAPSHOT_DIR=/path/to/snapshot PARQUET_DIR=/path/to/out python flatten-openalex-parquet.py

Dependencies:
    pip install pyarrow
"""

import glob
import gzip
import json
import os

import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SNAPSHOT_DIR = os.environ.get('SNAPSHOT_DIR', 'openalex-snapshot')
PARQUET_DIR  = os.environ.get('PARQUET_DIR',  'parquet-files')

# Set to a positive integer to process only that many partition files per
# entity (mirrors the OPENALEX_DEMO_FILES_PER_ENTITY env-var in the original).
FILES_PER_ENTITY = int(os.environ.get('OPENALEX_DEMO_FILES_PER_ENTITY', '0'))

# Rows to accumulate in memory before flushing to Parquet.
# Larger batches = better compression & fewer row-groups; tune to your RAM.
BATCH_SIZE = int(os.environ.get('OPENALEX_BATCH_SIZE', '50000'))

os.makedirs(PARQUET_DIR, exist_ok=True)


# ---------------------------------------------------------------------------
# Schema definitions
# Mirrors openalex-pg-schema.sql column order and types as closely as
# possible so DuckDB sees identical column names.
# pa.large_utf8() is used for columns that can hold very long text (abstracts,
# URLs, …); pa.utf8() is fine for short identifiers / codes.
# ---------------------------------------------------------------------------

SCHEMAS = {
    # ------------------------------------------------------------------
    # authors
    # ------------------------------------------------------------------
    'authors': pa.schema([
        ('id',                        pa.utf8()),
        ('orcid',                     pa.utf8()),
        ('display_name',              pa.utf8()),
        ('display_name_alternatives', pa.utf8()),   # JSON-serialised list
        ('works_count',               pa.int32()),
        ('cited_by_count',            pa.int32()),
        ('last_known_institution',    pa.utf8()),
        ('works_api_url',             pa.utf8()),
        ('updated_date',              pa.utf8()),
    ]),
    'authors_ids': pa.schema([
        ('author_id', pa.utf8()),
        ('openalex',  pa.utf8()),
        ('orcid',     pa.utf8()),
        ('scopus',    pa.utf8()),
        ('twitter',   pa.utf8()),
        ('wikipedia', pa.utf8()),
        ('mag',       pa.int64()),
    ]),
    'authors_counts_by_year': pa.schema([
        ('author_id',     pa.utf8()),
        ('year',          pa.int32()),
        ('works_count',   pa.int32()),
        ('cited_by_count',pa.int32()),
        ('oa_works_count',pa.int32()),
    ]),

    # ------------------------------------------------------------------
    # topics
    # ------------------------------------------------------------------
    'topics': pa.schema([
        ('id',                   pa.utf8()),
        ('display_name',         pa.utf8()),
        ('subfield_id',          pa.utf8()),
        ('subfield_display_name',pa.utf8()),
        ('field_id',             pa.utf8()),
        ('field_display_name',   pa.utf8()),
        ('domain_id',            pa.utf8()),
        ('domain_display_name',  pa.utf8()),
        ('description',          pa.large_utf8()),
        ('keywords',             pa.utf8()),
        ('works_api_url',        pa.utf8()),
        ('wikipedia_id',         pa.utf8()),
        ('works_count',          pa.int32()),
        ('cited_by_count',       pa.int32()),
        ('updated_date',         pa.utf8()),
        ('siblings',             pa.utf8()),   # JSON-serialised list
    ]),

    # ------------------------------------------------------------------
    # concepts
    # ------------------------------------------------------------------
    'concepts': pa.schema([
        ('id',                   pa.utf8()),
        ('wikidata',             pa.utf8()),
        ('display_name',         pa.utf8()),
        ('level',                pa.int32()),
        ('description',          pa.large_utf8()),
        ('works_count',          pa.int32()),
        ('cited_by_count',       pa.int32()),
        ('image_url',            pa.utf8()),
        ('image_thumbnail_url',  pa.utf8()),
        ('works_api_url',        pa.utf8()),
        ('updated_date',         pa.utf8()),
    ]),
    'concepts_ancestors': pa.schema([
        ('concept_id',  pa.utf8()),
        ('ancestor_id', pa.utf8()),
    ]),
    'concepts_counts_by_year': pa.schema([
        ('concept_id',    pa.utf8()),
        ('year',          pa.int32()),
        ('works_count',   pa.int32()),
        ('cited_by_count',pa.int32()),
        ('oa_works_count',pa.int32()),
    ]),
    'concepts_ids': pa.schema([
        ('concept_id', pa.utf8()),
        ('openalex',   pa.utf8()),
        ('wikidata',   pa.utf8()),
        ('wikipedia',  pa.utf8()),
        ('umls_aui',   pa.utf8()),   # JSON-serialised list
        ('umls_cui',   pa.utf8()),   # JSON-serialised list
        ('mag',        pa.int64()),
    ]),
    'concepts_related_concepts': pa.schema([
        ('concept_id',         pa.utf8()),
        ('related_concept_id', pa.utf8()),
        ('score',              pa.float32()),
    ]),

    # ------------------------------------------------------------------
    # institutions
    # ------------------------------------------------------------------
    'institutions': pa.schema([
        ('id',                        pa.utf8()),
        ('ror',                       pa.utf8()),
        ('display_name',              pa.utf8()),
        ('country_code',              pa.utf8()),
        ('type',                      pa.utf8()),
        ('homepage_url',              pa.utf8()),
        ('image_url',                 pa.utf8()),
        ('image_thumbnail_url',       pa.utf8()),
        ('display_name_acronyms',     pa.utf8()),   # JSON
        ('display_name_alternatives', pa.utf8()),   # JSON
        ('works_count',               pa.int32()),
        ('cited_by_count',            pa.int32()),
        ('works_api_url',             pa.utf8()),
        ('updated_date',              pa.utf8()),
    ]),
    'institutions_ids': pa.schema([
        ('institution_id', pa.utf8()),
        ('openalex',       pa.utf8()),
        ('ror',            pa.utf8()),
        ('grid',           pa.utf8()),
        ('wikipedia',      pa.utf8()),
        ('wikidata',       pa.utf8()),
        ('mag',            pa.int64()),
    ]),
    'institutions_geo': pa.schema([
        ('institution_id',  pa.utf8()),
        ('city',            pa.utf8()),
        ('geonames_city_id',pa.utf8()),
        ('region',          pa.utf8()),
        ('country_code',    pa.utf8()),
        ('country',         pa.utf8()),
        ('latitude',        pa.float32()),
        ('longitude',       pa.float32()),
    ]),
    'institutions_associated_institutions': pa.schema([
        ('institution_id',            pa.utf8()),
        ('associated_institution_id', pa.utf8()),
        ('relationship',              pa.utf8()),
    ]),
    'institutions_counts_by_year': pa.schema([
        ('institution_id', pa.utf8()),
        ('year',           pa.int32()),
        ('works_count',    pa.int32()),
        ('cited_by_count', pa.int32()),
        ('oa_works_count', pa.int32()),
    ]),

    # ------------------------------------------------------------------
    # publishers
    # ------------------------------------------------------------------
    'publishers': pa.schema([
        ('id',               pa.utf8()),
        ('display_name',     pa.utf8()),
        ('alternate_titles', pa.utf8()),   # JSON
        ('country_codes',    pa.utf8()),   # JSON
        ('hierarchy_level',  pa.int32()),
        ('parent_publisher', pa.utf8()),
        ('works_count',      pa.int32()),
        ('cited_by_count',   pa.int32()),
        ('sources_api_url',  pa.utf8()),
        ('updated_date',     pa.utf8()),
    ]),
    'publishers_ids': pa.schema([
        ('publisher_id', pa.utf8()),
        ('openalex',     pa.utf8()),
        ('ror',          pa.utf8()),
        ('wikidata',     pa.utf8()),
    ]),
    'publishers_counts_by_year': pa.schema([
        ('publisher_id',  pa.utf8()),
        ('year',          pa.int32()),
        ('works_count',   pa.int32()),
        ('cited_by_count',pa.int32()),
        ('oa_works_count',pa.int32()),
    ]),

    # ------------------------------------------------------------------
    # sources
    # ------------------------------------------------------------------
    'sources': pa.schema([
        ('id',            pa.utf8()),
        ('issn_l',        pa.utf8()),
        ('issn',          pa.utf8()),   # JSON
        ('display_name',  pa.utf8()),
        ('publisher',     pa.utf8()),
        ('works_count',   pa.int32()),
        ('cited_by_count',pa.int32()),
        ('is_oa',         pa.bool_()),
        ('is_in_doaj',    pa.bool_()),
        ('homepage_url',  pa.utf8()),
        ('works_api_url', pa.utf8()),
        ('updated_date',  pa.utf8()),
    ]),
    'sources_ids': pa.schema([
        ('source_id', pa.utf8()),
        ('openalex',  pa.utf8()),
        ('issn_l',    pa.utf8()),
        ('issn',      pa.utf8()),   # JSON
        ('mag',       pa.int64()),
        ('wikidata',  pa.utf8()),
        ('fatcat',    pa.utf8()),
    ]),
    'sources_counts_by_year': pa.schema([
        ('source_id',     pa.utf8()),
        ('year',          pa.int32()),
        ('works_count',   pa.int32()),
        ('cited_by_count',pa.int32()),
        ('oa_works_count',pa.int32()),
    ]),

    # ------------------------------------------------------------------
    # works  (13 child tables)
    # ------------------------------------------------------------------
    'works': pa.schema([
        ('id',                      pa.utf8()),
        ('doi',                     pa.utf8()),
        ('title',                   pa.large_utf8()),
        ('display_name',            pa.large_utf8()),
        ('publication_year',        pa.int32()),
        ('publication_date',        pa.utf8()),
        ('type',                    pa.utf8()),
        ('cited_by_count',          pa.int32()),
        ('is_retracted',            pa.bool_()),
        ('is_paratext',             pa.bool_()),
        ('cited_by_api_url',        pa.utf8()),
        ('abstract_inverted_index', pa.large_utf8()),  # JSON
        ('language',                pa.utf8()),
    ]),
    'works_primary_locations': pa.schema([
        ('work_id',          pa.utf8()),
        ('source_id',        pa.utf8()),
        ('landing_page_url', pa.utf8()),
        ('pdf_url',          pa.utf8()),
        ('is_oa',            pa.bool_()),
        ('version',          pa.utf8()),
        ('license',          pa.utf8()),
    ]),
    'works_locations': pa.schema([
        ('work_id',          pa.utf8()),
        ('source_id',        pa.utf8()),
        ('landing_page_url', pa.utf8()),
        ('pdf_url',          pa.utf8()),
        ('is_oa',            pa.bool_()),
        ('version',          pa.utf8()),
        ('license',          pa.utf8()),
    ]),
    'works_best_oa_locations': pa.schema([
        ('work_id',          pa.utf8()),
        ('source_id',        pa.utf8()),
        ('landing_page_url', pa.utf8()),
        ('pdf_url',          pa.utf8()),
        ('is_oa',            pa.bool_()),
        ('version',          pa.utf8()),
        ('license',          pa.utf8()),
    ]),
    'works_authorships': pa.schema([
        ('work_id',               pa.utf8()),
        ('author_position',       pa.utf8()),
        ('author_id',             pa.utf8()),
        ('institution_id',        pa.utf8()),
        ('raw_affiliation_string',pa.large_utf8()),
    ]),
    'works_biblio': pa.schema([
        ('work_id',    pa.utf8()),
        ('volume',     pa.utf8()),
        ('issue',      pa.utf8()),
        ('first_page', pa.utf8()),
        ('last_page',  pa.utf8()),
    ]),
    'works_topics': pa.schema([
        ('work_id',  pa.utf8()),
        ('topic_id', pa.utf8()),
        ('score',    pa.float32()),
    ]),
    'works_concepts': pa.schema([
        ('work_id',   pa.utf8()),
        ('concept_id',pa.utf8()),
        ('score',     pa.float32()),
    ]),
    'works_ids': pa.schema([
        ('work_id',  pa.utf8()),
        ('openalex', pa.utf8()),
        ('doi',      pa.utf8()),
        ('mag',      pa.int64()),
        ('pmid',     pa.utf8()),
        ('pmcid',    pa.utf8()),
    ]),
    'works_mesh': pa.schema([
        ('work_id',        pa.utf8()),
        ('descriptor_ui',  pa.utf8()),
        ('descriptor_name',pa.utf8()),
        ('qualifier_ui',   pa.utf8()),
        ('qualifier_name', pa.utf8()),
        ('is_major_topic', pa.bool_()),
    ]),
    'works_open_access': pa.schema([
        ('work_id',                   pa.utf8()),
        ('is_oa',                     pa.bool_()),
        ('oa_status',                 pa.utf8()),
        ('oa_url',                    pa.utf8()),
        ('any_repository_has_fulltext',pa.bool_()),
    ]),
    'works_referenced_works': pa.schema([
        ('work_id',            pa.utf8()),
        ('referenced_work_id', pa.utf8()),
    ]),
    'works_related_works': pa.schema([
        ('work_id',        pa.utf8()),
        ('related_work_id',pa.utf8()),
    ]),
}


# ---------------------------------------------------------------------------
# Helper: incremental Parquet writer
# ---------------------------------------------------------------------------

class ParquetBuffer:
    """
    Accumulates row dicts in memory and flushes to a single Parquet file
    once `batch_size` rows are ready (or on close).  All batches are
    appended to the same file via ParquetWriter so the result is one file
    per logical table — identical to the original one-CSV-per-table design.
    """

    def __init__(self, table_name: str, schema: pa.Schema,
                 batch_size: int = BATCH_SIZE):
        self.table_name = table_name
        self.schema = schema
        self.batch_size = batch_size
        self.path = os.path.join(PARQUET_DIR, f'{table_name}.parquet')
        self._rows: list[dict] = []
        self._writer: pq.ParquetWriter | None = None

    # ------------------------------------------------------------------
    def _open_writer(self):
        if self._writer is None:
            self._writer = pq.ParquetWriter(
                self.path, self.schema,
                compression='snappy',   # fast + good ratio; swap for 'zstd' if preferred
                write_statistics=True,
            )

    def _flush(self):
        if not self._rows:
            return
        self._open_writer()
        # Build column-oriented arrays with explicit casting to match the schema
        arrays = []
        for field in self.schema:
            col = [row.get(field.name) for row in self._rows]
            try:
                arrays.append(pa.array(col, type=field.type, safe=False))
            except (pa.ArrowInvalid, pa.ArrowTypeError):
                # Fall back to letting Arrow infer then cast
                arrays.append(pa.array(col).cast(field.type, safe=False))
        batch = pa.record_batch(arrays, schema=self.schema)
        self._writer.write_batch(batch)
        self._rows.clear()

    # ------------------------------------------------------------------
    def write(self, row: dict):
        self._rows.append(row)
        if len(self._rows) >= self.batch_size:
            self._flush()

    def close(self):
        self._flush()
        if self._writer:
            self._writer.close()
            self._writer = None
        print(f'  -> wrote {self.path}')


# ---------------------------------------------------------------------------
# Flatten helpers (one function per entity group, mirroring the original)
# ---------------------------------------------------------------------------

def _glob(entity: str):
    """Return sorted partition file paths for a given entity directory."""
    pattern = os.path.join(SNAPSHOT_DIR, 'data', entity, '*', '*.gz')
    return sorted(glob.glob(pattern))


def _iter_lines(jsonl_file_name: str):
    """Yield parsed JSON objects from a gzipped JSONL file."""
    with gzip.open(jsonl_file_name, 'r') as fh:
        for line in fh:
            if line.strip():
                yield json.loads(line)


def flatten_authors():
    print('Flattening: authors')
    bufs = {k: ParquetBuffer(k, SCHEMAS[k]) for k in
            ('authors', 'authors_ids', 'authors_counts_by_year')}
    try:
        files_done = 0
        for path in _glob('authors'):
            print(f'  {path}')
            for author in _iter_lines(path):
                author_id = author.get('id')
                if not author_id:
                    continue

                # authors (main table)
                bufs['authors'].write({
                    'id':                        author_id,
                    'orcid':                     author.get('orcid'),
                    'display_name':              author.get('display_name'),
                    'display_name_alternatives': json.dumps(
                        author.get('display_name_alternatives'),
                        ensure_ascii=False),
                    'works_count':               author.get('works_count'),
                    'cited_by_count':            author.get('cited_by_count'),
                    'last_known_institution':    (
                        author.get('last_known_institution') or {}).get('id'),
                    'works_api_url':             author.get('works_api_url'),
                    'updated_date':              author.get('updated_date'),
                })

                # authors_ids
                if ids := author.get('ids'):
                    bufs['authors_ids'].write({
                        'author_id': author_id,
                        'openalex':  ids.get('openalex'),
                        'orcid':     ids.get('orcid'),
                        'scopus':    ids.get('scopus'),
                        'twitter':   ids.get('twitter'),
                        'wikipedia': ids.get('wikipedia'),
                        'mag':       ids.get('mag'),
                    })

                # authors_counts_by_year
                for row in author.get('counts_by_year', []):
                    bufs['authors_counts_by_year'].write({
                        'author_id':      author_id,
                        'year':           row.get('year'),
                        'works_count':    row.get('works_count'),
                        'cited_by_count': row.get('cited_by_count'),
                        'oa_works_count': row.get('oa_works_count'),
                    })

            files_done += 1
            if FILES_PER_ENTITY and files_done >= FILES_PER_ENTITY:
                break
    finally:
        for b in bufs.values():
            b.close()


def flatten_topics():
    print('Flattening: topics')
    buf = ParquetBuffer('topics', SCHEMAS['topics'])
    seen = set()
    try:
        files_done = 0
        for path in _glob('topics'):
            print(f'  {path}')
            for topic in _iter_lines(path):
                topic_id = topic.get('id')
                if not topic_id or topic_id in seen:
                    continue
                seen.add(topic_id)

                # Flatten nested subfield / field / domain objects
                row = {
                    'id':                   topic_id,
                    'display_name':         topic.get('display_name'),
                    'subfield_id':          (topic.get('subfield') or {}).get('id'),
                    'subfield_display_name':(topic.get('subfield') or {}).get('display_name'),
                    'field_id':             (topic.get('field') or {}).get('id'),
                    'field_display_name':   (topic.get('field') or {}).get('display_name'),
                    'domain_id':            (topic.get('domain') or {}).get('id'),
                    'domain_display_name':  (topic.get('domain') or {}).get('display_name'),
                    'description':          topic.get('description'),
                    'keywords':             '; '.join(topic.get('keywords') or []),
                    'works_api_url':        topic.get('works_api_url'),
                    'wikipedia_id':         (topic.get('ids') or {}).get('wikipedia'),
                    'works_count':          topic.get('works_count'),
                    'cited_by_count':       topic.get('cited_by_count'),
                    'updated_date':         topic.get('updated') or topic.get('updated_date'),
                    'siblings':             json.dumps(topic.get('siblings'), ensure_ascii=False),
                }
                buf.write(row)

            files_done += 1
            if FILES_PER_ENTITY and files_done >= FILES_PER_ENTITY:
                break
    finally:
        buf.close()


def flatten_concepts():
    print('Flattening: concepts')
    bufs = {k: ParquetBuffer(k, SCHEMAS[k]) for k in (
        'concepts', 'concepts_ancestors', 'concepts_counts_by_year',
        'concepts_ids', 'concepts_related_concepts',
    )}
    seen = set()
    try:
        files_done = 0
        for path in _glob('concepts'):
            print(f'  {path}')
            for concept in _iter_lines(path):
                concept_id = concept.get('id')
                if not concept_id or concept_id in seen:
                    continue
                seen.add(concept_id)

                bufs['concepts'].write({
                    'id':                  concept_id,
                    'wikidata':            concept.get('wikidata'),
                    'display_name':        concept.get('display_name'),
                    'level':               concept.get('level'),
                    'description':         concept.get('description'),
                    'works_count':         concept.get('works_count'),
                    'cited_by_count':      concept.get('cited_by_count'),
                    'image_url':           concept.get('image_url'),
                    'image_thumbnail_url': concept.get('image_thumbnail_url'),
                    'works_api_url':       concept.get('works_api_url'),
                    'updated_date':        concept.get('updated_date'),
                })

                if ids := concept.get('ids'):
                    bufs['concepts_ids'].write({
                        'concept_id': concept_id,
                        'openalex':   ids.get('openalex'),
                        'wikidata':   ids.get('wikidata'),
                        'wikipedia':  ids.get('wikipedia'),
                        'umls_aui':   json.dumps(ids.get('umls_aui'), ensure_ascii=False),
                        'umls_cui':   json.dumps(ids.get('umls_cui'), ensure_ascii=False),
                        'mag':        ids.get('mag'),
                    })

                for ancestor in concept.get('ancestors', []):
                    if ancestor_id := ancestor.get('id'):
                        bufs['concepts_ancestors'].write({
                            'concept_id':  concept_id,
                            'ancestor_id': ancestor_id,
                        })

                for row in concept.get('counts_by_year', []):
                    bufs['concepts_counts_by_year'].write({
                        'concept_id':    concept_id,
                        'year':          row.get('year'),
                        'works_count':   row.get('works_count'),
                        'cited_by_count':row.get('cited_by_count'),
                        'oa_works_count':row.get('oa_works_count'),
                    })

                for related in concept.get('related_concepts', []):
                    if related_id := related.get('id'):
                        bufs['concepts_related_concepts'].write({
                            'concept_id':         concept_id,
                            'related_concept_id': related_id,
                            'score':              related.get('score'),
                        })

            files_done += 1
            if FILES_PER_ENTITY and files_done >= FILES_PER_ENTITY:
                break
    finally:
        for b in bufs.values():
            b.close()


def flatten_institutions():
    print('Flattening: institutions')
    bufs = {k: ParquetBuffer(k, SCHEMAS[k]) for k in (
        'institutions', 'institutions_ids', 'institutions_geo',
        'institutions_associated_institutions', 'institutions_counts_by_year',
    )}
    seen = set()
    try:
        files_done = 0
        for path in _glob('institutions'):
            print(f'  {path}')
            for inst in _iter_lines(path):
                inst_id = inst.get('id')
                if not inst_id or inst_id in seen:
                    continue
                seen.add(inst_id)

                bufs['institutions'].write({
                    'id':                        inst_id,
                    'ror':                       inst.get('ror'),
                    'display_name':              inst.get('display_name'),
                    'country_code':              inst.get('country_code'),
                    'type':                      inst.get('type'),
                    'homepage_url':              inst.get('homepage_url'),
                    'image_url':                 inst.get('image_url'),
                    'image_thumbnail_url':       inst.get('image_thumbnail_url'),
                    'display_name_acronyms':     json.dumps(
                        inst.get('display_name_acronyms'), ensure_ascii=False),
                    'display_name_alternatives': json.dumps(
                        inst.get('display_name_alternatives'), ensure_ascii=False),
                    'works_count':               inst.get('works_count'),
                    'cited_by_count':            inst.get('cited_by_count'),
                    'works_api_url':             inst.get('works_api_url'),
                    'updated_date':              inst.get('updated_date'),
                })

                if ids := inst.get('ids'):
                    bufs['institutions_ids'].write({
                        'institution_id': inst_id,
                        'openalex':       ids.get('openalex'),
                        'ror':            ids.get('ror'),
                        'grid':           ids.get('grid'),
                        'wikipedia':      ids.get('wikipedia'),
                        'wikidata':       ids.get('wikidata'),
                        'mag':            ids.get('mag'),
                    })

                if geo := inst.get('geo'):
                    bufs['institutions_geo'].write({
                        'institution_id':   inst_id,
                        'city':             geo.get('city'),
                        'geonames_city_id': geo.get('geonames_city_id'),
                        'region':           geo.get('region'),
                        'country_code':     geo.get('country_code'),
                        'country':          geo.get('country'),
                        'latitude':         geo.get('latitude'),
                        'longitude':        geo.get('longitude'),
                    })

                # Note: original script has a typo fallback ('associated_insitutions')
                associated = (
                    inst.get('associated_institutions')
                    or inst.get('associated_insitutions')
                    or []
                )
                for assoc in associated:
                    if assoc_id := assoc.get('id'):
                        bufs['institutions_associated_institutions'].write({
                            'institution_id':            inst_id,
                            'associated_institution_id': assoc_id,
                            'relationship':              assoc.get('relationship'),
                        })

                for row in inst.get('counts_by_year', []):
                    bufs['institutions_counts_by_year'].write({
                        'institution_id': inst_id,
                        'year':           row.get('year'),
                        'works_count':    row.get('works_count'),
                        'cited_by_count': row.get('cited_by_count'),
                        'oa_works_count': row.get('oa_works_count'),
                    })

            files_done += 1
            if FILES_PER_ENTITY and files_done >= FILES_PER_ENTITY:
                break
    finally:
        for b in bufs.values():
            b.close()


def flatten_publishers():
    print('Flattening: publishers')
    bufs = {k: ParquetBuffer(k, SCHEMAS[k]) for k in (
        'publishers', 'publishers_ids', 'publishers_counts_by_year',
    )}
    seen = set()
    try:
        files_done = 0
        for path in _glob('publishers'):
            print(f'  {path}')
            for pub in _iter_lines(path):
                pub_id = pub.get('id')
                if not pub_id or pub_id in seen:
                    continue
                seen.add(pub_id)

                bufs['publishers'].write({
                    'id':               pub_id,
                    'display_name':     pub.get('display_name'),
                    'alternate_titles': json.dumps(
                        pub.get('alternate_titles'), ensure_ascii=False),
                    'country_codes':    json.dumps(
                        pub.get('country_codes'), ensure_ascii=False),
                    'hierarchy_level':  pub.get('hierarchy_level'),
                    'parent_publisher': (pub.get('parent_publisher') or {}).get('id')
                                        if isinstance(pub.get('parent_publisher'), dict)
                                        else pub.get('parent_publisher'),
                    'works_count':      pub.get('works_count'),
                    'cited_by_count':   pub.get('cited_by_count'),
                    'sources_api_url':  pub.get('sources_api_url'),
                    'updated_date':     pub.get('updated_date'),
                })

                if ids := pub.get('ids'):
                    bufs['publishers_ids'].write({
                        'publisher_id': pub_id,
                        'openalex':     ids.get('openalex'),
                        'ror':          ids.get('ror'),
                        'wikidata':     ids.get('wikidata'),
                    })

                for row in pub.get('counts_by_year', []):
                    bufs['publishers_counts_by_year'].write({
                        'publisher_id':  pub_id,
                        'year':          row.get('year'),
                        'works_count':   row.get('works_count'),
                        'cited_by_count':row.get('cited_by_count'),
                        'oa_works_count':row.get('oa_works_count'),
                    })

            files_done += 1
            if FILES_PER_ENTITY and files_done >= FILES_PER_ENTITY:
                break
    finally:
        for b in bufs.values():
            b.close()


def flatten_sources():
    print('Flattening: sources')
    bufs = {k: ParquetBuffer(k, SCHEMAS[k]) for k in (
        'sources', 'sources_ids', 'sources_counts_by_year',
    )}
    seen = set()
    try:
        files_done = 0
        for path in _glob('sources'):
            print(f'  {path}')
            for src in _iter_lines(path):
                src_id = src.get('id')
                if not src_id or src_id in seen:
                    continue
                seen.add(src_id)

                bufs['sources'].write({
                    'id':            src_id,
                    'issn_l':        src.get('issn_l'),
                    'issn':          json.dumps(src.get('issn')),
                    'display_name':  src.get('display_name'),
                    'publisher':     src.get('publisher'),
                    'works_count':   src.get('works_count'),
                    'cited_by_count':src.get('cited_by_count'),
                    'is_oa':         src.get('is_oa'),
                    'is_in_doaj':    src.get('is_in_doaj'),
                    'homepage_url':  src.get('homepage_url'),
                    'works_api_url': src.get('works_api_url'),
                    'updated_date':  src.get('updated_date'),
                })

                if ids := src.get('ids'):
                    bufs['sources_ids'].write({
                        'source_id': src_id,
                        'openalex':  ids.get('openalex'),
                        'issn_l':    ids.get('issn_l'),
                        'issn':      json.dumps(ids.get('issn')),
                        'mag':       ids.get('mag'),
                        'wikidata':  ids.get('wikidata'),
                        'fatcat':    ids.get('fatcat'),
                    })

                for row in src.get('counts_by_year', []):
                    bufs['sources_counts_by_year'].write({
                        'source_id':     src_id,
                        'year':          row.get('year'),
                        'works_count':   row.get('works_count'),
                        'cited_by_count':row.get('cited_by_count'),
                        'oa_works_count':row.get('oa_works_count'),
                    })

            files_done += 1
            if FILES_PER_ENTITY and files_done >= FILES_PER_ENTITY:
                break
    finally:
        for b in bufs.values():
            b.close()


def flatten_works():
    print('Flattening: works')
    table_names = (
        'works', 'works_primary_locations', 'works_locations',
        'works_best_oa_locations', 'works_authorships', 'works_biblio',
        'works_topics', 'works_concepts', 'works_ids', 'works_mesh',
        'works_open_access', 'works_referenced_works', 'works_related_works',
    )
    bufs = {k: ParquetBuffer(k, SCHEMAS[k]) for k in table_names}
    try:
        files_done = 0
        for path in _glob('works'):
            print(f'  {path}')
            for work in _iter_lines(path):
                work_id = work.get('id')
                if not work_id:
                    continue

                # works (main table)
                abstract = work.get('abstract_inverted_index')
                bufs['works'].write({
                    'id':                      work_id,
                    'doi':                     work.get('doi'),
                    'title':                   work.get('title'),
                    'display_name':            work.get('display_name'),
                    'publication_year':        work.get('publication_year'),
                    'publication_date':        work.get('publication_date'),
                    'type':                    work.get('type'),
                    'cited_by_count':          work.get('cited_by_count'),
                    'is_retracted':            work.get('is_retracted'),
                    'is_paratext':             work.get('is_paratext'),
                    'cited_by_api_url':        work.get('cited_by_api_url'),
                    'abstract_inverted_index': json.dumps(abstract, ensure_ascii=False)
                                               if abstract is not None else None,
                    'language':                work.get('language'),
                })

                def _location_row(loc):
                    src = loc.get('source') or {}
                    return {
                        'work_id':          work_id,
                        'source_id':        src.get('id'),
                        'landing_page_url': loc.get('landing_page_url'),
                        'pdf_url':          loc.get('pdf_url'),
                        'is_oa':            loc.get('is_oa'),
                        'version':          loc.get('version'),
                        'license':          loc.get('license'),
                    }

                # works_primary_locations
                if pl := (work.get('primary_location') or {}):
                    if (pl.get('source') or {}).get('id'):
                        bufs['works_primary_locations'].write(_location_row(pl))

                # works_locations
                for loc in work.get('locations', []):
                    if (loc.get('source') or {}).get('id'):
                        bufs['works_locations'].write(_location_row(loc))

                # works_best_oa_locations
                if boa := (work.get('best_oa_location') or {}):
                    if (boa.get('source') or {}).get('id'):
                        bufs['works_best_oa_locations'].write(_location_row(boa))

                # works_authorships
                for authorship in work.get('authorships', []):
                    author_id = (authorship.get('author') or {}).get('id')
                    if not author_id:
                        continue
                    institutions = authorship.get('institutions') or []
                    inst_ids = [i.get('id') for i in institutions if i.get('id')]
                    inst_ids = inst_ids or [None]
                    for inst_id in inst_ids:
                        bufs['works_authorships'].write({
                            'work_id':                work_id,
                            'author_position':        authorship.get('author_position'),
                            'author_id':              author_id,
                            'institution_id':         inst_id,
                            'raw_affiliation_string': authorship.get('raw_affiliation_string'),
                        })

                # works_biblio
                if biblio := work.get('biblio'):
                    bufs['works_biblio'].write({
                        'work_id':    work_id,
                        'volume':     biblio.get('volume'),
                        'issue':      biblio.get('issue'),
                        'first_page': biblio.get('first_page'),
                        'last_page':  biblio.get('last_page'),
                    })

                # works_topics
                for topic in work.get('topics', []):
                    if topic_id := topic.get('id'):
                        bufs['works_topics'].write({
                            'work_id':  work_id,
                            'topic_id': topic_id,
                            'score':    topic.get('score'),
                        })

                # works_concepts
                for concept in work.get('concepts', []):
                    if concept_id := concept.get('id'):
                        bufs['works_concepts'].write({
                            'work_id':   work_id,
                            'concept_id':concept_id,
                            'score':     concept.get('score'),
                        })

                # works_ids
                if ids := work.get('ids'):
                    bufs['works_ids'].write({
                        'work_id':  work_id,
                        'openalex': ids.get('openalex'),
                        'doi':      ids.get('doi'),
                        'mag':      ids.get('mag'),
                        'pmid':     ids.get('pmid'),
                        'pmcid':    ids.get('pmcid'),
                    })

                # works_mesh
                for mesh in work.get('mesh', []):
                    bufs['works_mesh'].write({
                        'work_id':        work_id,
                        'descriptor_ui':  mesh.get('descriptor_ui'),
                        'descriptor_name':mesh.get('descriptor_name'),
                        'qualifier_ui':   mesh.get('qualifier_ui'),
                        'qualifier_name': mesh.get('qualifier_name'),
                        'is_major_topic': mesh.get('is_major_topic'),
                    })

                # works_open_access
                if oa := work.get('open_access'):
                    bufs['works_open_access'].write({
                        'work_id':                    work_id,
                        'is_oa':                      oa.get('is_oa'),
                        'oa_status':                  oa.get('oa_status'),
                        'oa_url':                     oa.get('oa_url'),
                        'any_repository_has_fulltext':oa.get('any_repository_has_fulltext'),
                    })

                # works_referenced_works
                for ref in work.get('referenced_works', []):
                    if ref:
                        bufs['works_referenced_works'].write({
                            'work_id':            work_id,
                            'referenced_work_id': ref,
                        })

                # works_related_works
                for rel in work.get('related_works', []):
                    if rel:
                        bufs['works_related_works'].write({
                            'work_id':         work_id,
                            'related_work_id': rel,
                        })

            files_done += 1
            if FILES_PER_ENTITY and files_done >= FILES_PER_ENTITY:
                break
    finally:
        for b in bufs.values():
            b.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    flatten_topics()
    flatten_authors()
    flatten_concepts()
    flatten_institutions()
    flatten_publishers()
    flatten_sources()
    flatten_works()
    print('\nDone.  Parquet files are in:', PARQUET_DIR)
