# CineInsight design and operating decisions

## Architecture and contracts

The supplied snapshot is immutable input. `lakehouse/landing/*.csv` are byte copies; initialization refuses to overwrite a changed copy. SHA-256 is recorded against every source batch. Bronze is append-only in SQLite: one row per source row and ingestion batch, retaining the original source fields as JSON alongside source system/file, batch ID, ingestion time, row number, and canonical record hash. Control rows record run bounds, checksums, status, row counts, and watermark. The implementation is intentionally a local reference: use object versioning/retention and Delta/Iceberg append tables plus transaction logs in production.

Silver event records are canonical `InteractionEvent` rows. The event identity is a SHA-256 of source system, event type, source user/movie, event timestamp and source value. Replaying a batch is an UPSERT on this ID. If conflicting representations of an identical identity arrive, the greatest event-time then lexicographically greatest record hash wins, making selection stable. Profiling verifies the entire rating file is nondecreasing by `(userId,movieId)` and finds zero repeated pairs in this snapshot; tag events can repeat user/movie/tag combinations. The timestamp remains part of the CDM event key so future sources that permit re-rating do not lose events. Duplicate event IDs collapse; source rows remain auditable in Bronze.

Event-time windows are calendar-year windows spanning the timestamps present in each source. The rating history begins in 1995; the tag events begin in 2005. The batch identity includes source and exact interval, so completion is resumable. `--as-of` limits processing for backfill. The stored watermark is the greatest completed interval end. Bronze row IDs and a Silver processing ledger allow an appended late record to enter its event-year batch and be merged once. Because a monolithic CSV is not seekable by event time, this local adapter scans the file to discover new rows; it writes/processes only unprocessed rows. Production should land immutable date-partitioned files and use a configurable overlap (for example 7 days), retaining event ID idempotency and advancing the watermark only after reconciliation and Silver commit.

Movie catalog CDC is synthetic and deterministic, clearly tagged `synthetic-cdc`: for every 5,000 catalog rows it emits one new ID insert, one genre update, and one soft delete. This is a demonstration feed, not a claim about actual catalog events. It is replay-safe by batch and row keys.

## CDM mapping

| CDM | Source mapping | Rule |
|---|---|---|
| `party_id` | `rating.userId`, `tag.userId` | integer source party identifier |
| `content_id` | `rating.movieId`, `tag.movieId` | integer MovieLens ID |
| `event_type` | table name | `RATING` or `TAG` |
| `event_value` | rating or tag | Rating converted to 0–100 (`rating * 20`); tag text is an event attribute and represented as null numeric value |
| `event_time_utc` | `timestamp` | observed file is `YYYY-MM-DD HH:mm:ss`; parse as UTC, while adapter also accepts Unix epoch |
| `content title/year/genres` | `movie.title`, `movie.genres` | trim title, regex terminal `(YYYY)`, split genres into JSON array; no-genre sentinel maps to empty array |

Additional sources map at the adapter boundary; the InteractionEvent schema remains stable. Free-text tags retain user spelling in source Bronze and are trimmed for validation; token normalization belongs in a separate text dimension if semantic equivalence is desired. Never silently merge `scifi` and `sci-fi` without a governed synonym map.

## Quality rules and quarantine

Blocking: unreadable source/checksum mismatch; invalid event timestamp; rating outside [0.5,5.0] or not on 0.5 increments; invalid required IDs. Warning: missing release-year suffix, no-genre sentinel, orphan catalog/link/genome references, empty optional IDs, suspicious extreme user/movie frequency, repeated event identity. Empty tags and malformed required event values go to `quarantine` with batch, source row, error code, reason and payload; source row remains in Bronze. Repair is a corrected source row in a later batch, then rerun the same Silver merge. DQ results are stored by batch/rule. Reconciliation compares full Landing checksum and parsed row count with the source expected count before marking a batch successful.

Profiling should include exact counts/nulls/type inference, PK/business-key duplicates, rating domain, event timestamp range, movie genre/year exceptions, foreign-key anti-joins, per-key event counts, and partition size. The source rating user set is broad; tags use a smaller user subset. For ratings, partition by event date (month for large tables), then cluster/bucket on content/user where supported; do not partition by userId because it creates many tiny partitions and skew. Tag by event month; movie/link/genome_tags are small dimensions; genome_scores by movieId range/hash or bucket and optionally tagId. Avoid one partition per category or high-cardinality ID. Monitor p99/max-to-median partition row ratio and skewed join keys.

### Observed profile of the supplied files

| Source | Rows | Observations |
|---|---:|---|
| rating | 20,000,263 | 0 empty cells; 138,493 users; 26,744 rated movies; legal half-star values; exact full-file `(userId,movieId)` uniqueness verified from sorted key order; events span 1995-01-09 to 2015-03-31 |
| tag | 465,564 | 7 blank/whitespace tags; quarantine as warning while retaining source rows |
| movie | 27,278 | 26 titles lack terminal year; 246 use `(no genres listed)` |
| link | 27,278 | 252 missing TMDb IDs; IMDb IDs present |
| genome_scores | 11,709,768 | Dense derived score table; range and foreign keys are checked on load |
| genome_tags | 1,128 | Small lookup dimension |

Rating skew is material: the heaviest user has 9,254 ratings versus a median of 68 per user; the busiest movie has 67,310 ratings. This supports date partitioning and explicit skew monitoring/salting for pathological joins, rather than partitioning by user or movie. Rating distribution peaks at 4.0 (5,561,926) and contains all ten expected half-star values.

## Dimensional model and SCD

```mermaid
erDiagram
  DIM_USER ||--o{ FACT_INTERACTION : party
  DIM_MOVIE ||--o{ FACT_INTERACTION : content_version
  DIM_MOVIE ||--o{ FACT_GENOME : describes
  DIM_GENOME_TAG ||--o{ FACT_GENOME : labels
  DIM_MOVIE ||--|| DIM_MOVIE_LINK : external_ids
  DIM_MOVIE { int movie_sk PK; int movie_id; string title; int release_year; string genres_json; datetime effective_from; datetime effective_to; bool is_current; int version }
  DIM_USER { int user_sk PK; int user_id; datetime first_seen }
  FACT_INTERACTION { string event_id PK; int user_sk FK; int movie_sk FK; string event_type; float event_value; datetime event_time_utc }
  FACT_GENOME { int movie_id FK; int tag_id FK; float relevance }
```

`fact_interaction` grain: one rating or tag event from one source at one event time. `fact_genome` grain: one derived relevance score per movie/genome tag. Movie and user surrogate keys isolate analytics from source key changes and enable historical joins. External source IDs are degenerate attributes in a 1:1 link dimension; missing external IDs remain null.

Movie `genres` is SCD2 because the requirement asks genre classification as of a past date and genre changes affect historical analysis. Soft delete is also versioned so historical events still resolve to the movie version active at event time. Corrected title and year use SCD1: the correction propagates across versions because it fixes a display/metadata error rather than a historical business state. `previous_title` and `changed_date` provide a Type 3 view of the immediately preceding label correction. Users do not exist as a source dimension in this dataset, so only inferred user members are created from observed event IDs; future user segmentation attributes should use Type 2 when historical cohort reporting matters, while typo/format fixes use Type 1.

Point-in-time join predicate: `event_time_utc >= effective_from AND (effective_to IS NULL OR event_time_utc < effective_to)`. A conventional current-row join answers what the movie is now and can misclassify historical genre. The initial movie row is an inferred member if an event arrives before its catalog row; later catalog arrival fills the member and event facts are re-resolved. This dataset has complete movie IDs for normal events, but this rule handles future feeds.

## Analytics choices

Movie ranking uses at least 100 ratings to reduce small-sample volatility; also publish rating count and variance. With no threshold, one-rating five-star titles dominate. Genre mean and population variance are weighted by rating-event membership, so a movie with several genres contributes to each genre. Report both release-year and rating-month trends; neither is causal. Normalize tags using Unicode NFKC, trim, casefold, whitespace collapse, and punctuation policy only in a governed derived field; the baseline pipeline retains raw tag text to avoid irreversible interpretation. Tag/rating correlation should be described as association, not causal influence. Genome coverage is number of catalog movies having at least one score divided by active catalog movies; genome relevance is algorithmically derived (not user-entered tag behavior). Hidden gems use quality >=4.0, 10–99 ratings and current non-deleted movies; thresholds are explicit tunable policy. IMDb reconstruction zero-pads to seven digits and TMDb URLs use the integer ID.

## Operations

Airflow DAG stages: landing verification → Bronze ingestion/reconciliation → Silver merge and blocking DQ → movie SCD/catalog merge → dimensions/facts → marts. Retries are safe because stage outputs are keyed and idempotent. Backfill takes a logical date and invokes the same bounded interval. In a Delta deployment, use transaction MERGE, partition pruning, optimize/Z-order on frequently filtered content/date, and retention-governed VACUUM. SQLite does not implement Delta transaction history, distributed scale, native schema evolution, or true table-level time travel; these remain production adapter responsibilities.
