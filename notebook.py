# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %sql
# MAGIC SELECT 1 AS connection_check

# COMMAND ----------

pwd


# COMMAND ----------

from pathlib import Path
import json

# Copy this Git folder's absolute path from its Workspace menu; include /Workspace/.
repo_root = Path("/Workspace/Users/jeffery.l.wall.irs@catalyte.ai/to6-databricks-challenge-starter")
assert (repo_root / "README.md").is_file(), "Check the Git folder path"
practice_result = spark.sql("SELECT 1 AS connection_check").first().asDict()
assert practice_result == {"connection_check": 1}
(repo_root / "outputs").mkdir(exist_ok=True)
(repo_root / "outputs/connection-practice.json").write_text(
    json.dumps(practice_result) + "\n", encoding="utf-8"
)

# COMMAND ----------

# DBTITLE 1,Setup: schemas and Git folder root
import hashlib
from pathlib import Path

INPUT_SCHEMA = "training.to6_challenge3_inputs"
INITIAL_TABLE = f"{INPUT_SCHEMA}.returns_initial"
CORRECTED_TABLE = f"{INPUT_SCHEMA}.returns_corrected"
LINEAGE_VIEW = f"{INPUT_SCHEMA}.source_lineage"
IN_CATALOG, IN_SCHEMA = INPUT_SCHEMA.split(".")
workspace_user = spark.sql("SELECT current_user() AS username").first()["username"]
OUTPUT_SCHEMA = "training.to6_c3_" + hashlib.sha256(workspace_user.encode("utf-8")).hexdigest()[:12]

# Git folder root, derived from this notebook's location so it works in any user's clone
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
REPO_ROOT = Path(_nb_path if _nb_path.startswith("/Workspace") else "/Workspace" + _nb_path).parent
assert (REPO_ROOT / "README.md").is_file(), f"Git folder root not found at {REPO_ROOT}"
print({"input_schema": INPUT_SCHEMA, "output_schema": OUTPUT_SCHEMA, "repo_root": str(REPO_ROOT)})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Understand the input
# MAGIC Record Catalog descriptions, sample observations and the supplied lineage source.

# COMMAND ----------

# DBTITLE 1,1a. Catalog descriptions of the inputs
# Catalog (Unity Catalog) table descriptions for the two deliveries and the lineage view
display(spark.sql(f"""
    SELECT table_catalog || '.' || table_schema || '.' || table_name AS full_name,
           table_type, comment
    FROM {IN_CATALOG}.information_schema.tables
    WHERE table_schema = '{IN_SCHEMA}'
    ORDER BY table_name
"""))

# Column-level types and comments (column comments are empty in this delivery)
display(spark.sql(f"DESCRIBE TABLE {INITIAL_TABLE}"))

# COMMAND ----------

# DBTITLE 1,1b. Supplied lineage view
# Supplied source map (separate from the Catalog UI Lineage tab)
display(spark.table(LINEAGE_VIEW))

# COMMAND ----------

# DBTITLE 1,1c. Sample rows from each batch
# At least three sample rows per batch, ordered for reproducibility
for name in (INITIAL_TABLE, CORRECTED_TABLE):
    print(name)
    display(spark.table(name).orderBy("tax_year", "return_id").limit(5))

# COMMAND ----------

# DBTITLE 1,1d. Full-batch profile: rows, distinct IDs, missing values, zero income
# Full-batch profile of both deliveries (all rows, not a sample)
profile_sql = """
SELECT '{label}' AS batch,
       COUNT(*)                                   AS row_count,
       COUNT(DISTINCT return_id)                  AS distinct_return_ids,
       COUNT(*) - COUNT(DISTINCT return_id)       AS extra_rows_from_duplicate_ids,
       COUNT(DISTINCT taxpayer_id)                AS distinct_taxpayers,
       SUM(IF(return_id IS NULL, 1, 0))           AS missing_return_id,
       SUM(IF(tax_year  IS NULL, 1, 0))           AS missing_tax_year,
       SUM(IF(income    IS NULL, 1, 0))           AS missing_income,
       SUM(IF(deduction IS NULL, 1, 0))           AS missing_deduction,
       SUM(IF(income = 0, 1, 0))                  AS zero_income_rows,
       SUM(IF(income < 0 OR deduction < 0, 1, 0)) AS negative_amount_rows,
       MIN(tax_year) AS min_year, MAX(tax_year) AS max_year
FROM {table}
"""
profile_df = spark.sql(
    profile_sql.format(label="initial", table=INITIAL_TABLE)
    + " UNION ALL "
    + profile_sql.format(label="corrected", table=CORRECTED_TABLE)
)
display(profile_df)

# COMMAND ----------

# DBTITLE 1,1e. Returns by tax year
# Rows and distinct return IDs per tax year, both batches side by side
display(spark.sql(f"""
    SELECT batch, tax_year,
           COUNT(*) AS row_count,
           COUNT(DISTINCT return_id) AS distinct_return_ids
    FROM (
        SELECT 'initial' AS batch, * FROM {INITIAL_TABLE}
        UNION ALL
        SELECT 'corrected' AS batch, * FROM {CORRECTED_TABLE}
    )
    GROUP BY batch, tax_year
    ORDER BY tax_year, batch DESC
"""))

# COMMAND ----------

# DBTITLE 1,1f. Records with missing required values
# Every row where a required field (return_id, tax_year, income, deduction) is null
missing_filter = "return_id IS NULL OR tax_year IS NULL OR income IS NULL OR deduction IS NULL"
for name in (INITIAL_TABLE, CORRECTED_TABLE):
    print(name)
    display(spark.table(name).where(missing_filter).orderBy("return_id"))

# COMMAND ----------

# DBTITLE 1,1g. Duplicate return IDs (all copies shown)
# All rows whose return_id occurs more than once, so the copies can be compared field by field
for name in (INITIAL_TABLE, CORRECTED_TABLE):
    print(name)
    display(spark.sql(f"""
        SELECT *, COUNT(*) OVER (PARTITION BY return_id) AS copies
        FROM {name}
        QUALIFY copies > 1
        ORDER BY return_id, tax_year
    """))

# COMMAND ----------

# DBTITLE 1,1h. Zero-income records
# Zero income is valid, but deduction / income is undefined for these rows
for name in (INITIAL_TABLE, CORRECTED_TABLE):
    print(name)
    display(spark.table(name).where("income = 0").orderBy("return_id"))

# COMMAND ----------

# DBTITLE 1,1i. Source reference and findings
# MAGIC %md
# MAGIC ### Source reference
# MAGIC * **Inputs:** `training.to6_challenge3_inputs.returns_initial` and `training.to6_challenge3_inputs.returns_corrected` (managed tables).
# MAGIC * **Upstream source (supplied lineage view `training.to6_challenge3_inputs.source_lineage`):** both deliveries were created by SQL from `training.to6_challenge3_inputs.parsed_source`. The initial batch is a "transformation; validation required"; the corrected batch must pass the "same validation rules". The view is a supplied source map, not a Catalog UI lineage check.
# MAGIC * **Catalog descriptions:** `parsed_source` says one row is one return, `return_id` identifies the return, `taxpayer_id` identifies a filer who may recur across years, amounts are nonnegative and zero income is valid. `returns_initial` has "known delivery defects"; `returns_corrected` is "one row per return". Column comments are empty, so column meaning comes from the table descriptions and CHALLENGE.md.
# MAGIC
# MAGIC ### Row meaning and fields
# MAGIC * **Row:** one tax return. **Uniqueness identifier:** `return_id` (must occur exactly once). `taxpayer_id` is *not* unique: the same filer appears in several years (e.g. F0 has R01 in 2022, R05 in 2023 and R09 in 2024).
# MAGIC * **Required fields:** `return_id`, `tax_year`, `income`, `deduction` (all nullable in the schema, so nulls are possible and must be checked). `taxpayer_id` is used only for understanding; it is excluded from features under need-to-know.
# MAGIC
# MAGIC ### Full-batch observations (all rows, from cells 1d–1h)
# MAGIC
# MAGIC | Observation | `returns_initial` | `returns_corrected` |
# MAGIC | --- | --- | --- |
# MAGIC | Rows / distinct `return_id` | 13 / 12 | 12 / 12 |
# MAGIC | Rows by year 2022 / 2023 / 2024 | 5 / 4 / 4 (2022 has 4 distinct IDs) | 4 / 4 / 4 |
# MAGIC | Missing required values | 1 row: R02 (F1, 2022) `income` is null | none |
# MAGIC | Duplicate `return_id` | R01 appears twice, both copies identical (F0, 2022, income 100, deduction 20) | none |
# MAGIC | Zero-income rows | R03 (2022, deduction 46), R09 (2024, deduction 124) | same R03 and R09 |
# MAGIC | Negative amounts | none | none |
# MAGIC | Distinct filers / year range | 4 / 2022–2024 | 4 / 2022–2024 |
# MAGIC
# MAGIC ### How these findings affect the next steps
# MAGIC * **Batch decision (step 2):** the initial batch must fail the completeness rule (R02 null `income`) and the uniqueness rule (R01 duplicated). Preserve both R01 copies and the R02 row with their rule and reason. Do not deduplicate R01 silently or fill R02's income with zero. The corrected batch carries R02 with income 200 from the delivery itself, so it is the replacement to assess with the same rules.
# MAGIC * **Ratio (step 3):** R03 and R09 have zero income, so the ratio must be computed null-safely (null when income = 0) and both rows kept. R01 (20 / 100 = 0.2) is a simple nonzero case for the expected-value check.
# MAGIC * **Splits (step 4):** with the corrected batch, expect 4 returns per year (12 in total). Every `return_id` must land in exactly one split table.
# MAGIC * **Later modelling:** the same filer appears in train, validation and test years. Dropping `taxpayer_id` does not remove this cross-year link, so later work should assess the leakage risk.
# MAGIC * These are exploratory observations only. The formal quality checks in step 2 still need to run and gate the output.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Decide which batch is usable
# MAGIC Keep the initial rejection evidence and apply the same checks to the replacement.

# COMMAND ----------

# DBTITLE 1,2a. Validation rules and batch gate
from pyspark.sql import functions as F, Window

# REPO_ROOT is defined once in the setup cell
REQUIRED_COLUMNS = ["return_id", "tax_year", "income", "deduction"]
AMOUNT_COLUMNS = ["income", "deduction"]
RECORD_COLUMNS = ["return_id", "taxpayer_id", "tax_year", "income", "deduction"]
TAX_YEARS = (2022, 2023, 2024)  # train / validation / test years
RULES = {
    "completeness": "return_id, tax_year, income and deduction must all be non-null",
    "uniqueness": "each return_id must occur exactly once",
    "nonnegative_amounts": "income and deduction must be >= 0 (zero is valid)",
    "tax_year_in_scope": "tax_year must be 2022, 2023 or 2024 so every return lands in exactly one split",
}
REJECTIONS_TABLE = f"{OUTPUT_SCHEMA}.initial_batch_rejections"
CHECK_RESULTS_TABLE = f"{OUTPUT_SCHEMA}.batch_check_results"


class BatchRejected(Exception):
    """Raised when a delivery violates a data-contract rule; no feature output may be written."""


def read_snapshot(table_name):
    """Pin the table's current Delta version so the checks and downstream use read the same data."""
    version = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1").first()["version"]
    return version, spark.read.option("versionAsOf", version).table(table_name)


def find_violations(table_name, batch_df):
    """Return one row per (record, broken rule) with the full original record and a reason."""
    df = (
        batch_df
        .withColumn("_missing", F.concat_ws(", ", *[F.when(F.col(c).isNull(), F.lit(c)) for c in REQUIRED_COLUMNS]))
        .withColumn("_negative", F.concat_ws(", ", *[F.when(F.col(c) < 0, F.lit(c)) for c in AMOUNT_COLUMNS]))
        .withColumn("_copies", F.count(F.lit(1)).over(Window.partitionBy("return_id")))
    )
    completeness = df.where(F.col("_missing") != "").select(
        F.lit("completeness").alias("rule"),
        F.concat(F.lit("required value missing: "), F.col("_missing")).alias("reason"),
        *RECORD_COLUMNS,
    )
    uniqueness = df.where(F.col("return_id").isNotNull() & (F.col("_copies") > 1)).select(
        F.lit("uniqueness").alias("rule"),
        F.concat(F.lit("return_id occurs "), F.col("_copies").cast("string"), F.lit(" times; must occur exactly once")).alias("reason"),
        *RECORD_COLUMNS,
    )
    nonnegative = df.where(F.col("_negative") != "").select(
        F.lit("nonnegative_amounts").alias("rule"),
        F.concat(F.lit("negative amount: "), F.col("_negative")).alias("reason"),
        *RECORD_COLUMNS,
    )
    out_of_scope = df.where(F.col("tax_year").isNotNull() & ~F.col("tax_year").isin(*TAX_YEARS)).select(
        F.lit("tax_year_in_scope").alias("rule"),
        F.concat(F.lit("tax_year "), F.col("tax_year").cast("string"), F.lit(" is outside 2022-2024")).alias("reason"),
        *RECORD_COLUMNS,
    )
    return (
        completeness.unionByName(uniqueness).unionByName(nonnegative).unionByName(out_of_scope)
        .select(F.lit(table_name).alias("source_table"), "*")
    )


def summarize_checks(table_name, version, batch_df, violations):
    """One row per rule: source version, rows checked, violating rows, PASS/FAIL."""
    counts = {r["rule"]: r["n"] for r in violations.groupBy("rule").agg(F.count("*").alias("n")).collect()}
    rows_checked = batch_df.count()
    return spark.createDataFrame(
        [(table_name, int(version), rule, RULES[rule], rows_checked, counts.get(rule, 0),
          "PASS" if counts.get(rule, 0) == 0 else "FAIL") for rule in RULES],
        "source_table string, source_version long, rule string, rule_definition string, "
        "rows_checked long, violating_rows long, result string",
    )


def gate_batch(table_name):
    """Return (version, DataFrame pinned to that version) only if every rule passes; otherwise raise BatchRejected."""
    version, batch_df = read_snapshot(table_name)
    violations = find_violations(table_name, batch_df)
    failed = sorted({r["rule"] for r in violations.select("rule").collect()})
    if failed:
        ids = sorted({r["return_id"] for r in violations.select("return_id").collect()}, key=str)
        raise BatchRejected(f"{table_name} (version {version}) rejected: failed rules {failed}; affected return_id(s) {ids}")
    return version, batch_df

# COMMAND ----------

# DBTITLE 1,2b. Check the initial batch and preserve rejected records
# Apply all rules to one pinned snapshot of the initial batch
initial_version, initial_df = read_snapshot(INITIAL_TABLE)
initial_violations = find_violations(INITIAL_TABLE, initial_df)
initial_summary = summarize_checks(INITIAL_TABLE, initial_version, initial_df, initial_violations)
display(initial_summary)

# Preserve every affected record (all copies of duplicates included) with its rule and reason.
# Overwrite keeps reruns idempotent; checked_at records when this rejection was made.
initial_rejections = (
    initial_violations
    .withColumn("source_version", F.lit(initial_version).cast("long"))
    .withColumn("checked_at", F.current_timestamp())
)
initial_rejections.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(REJECTIONS_TABLE)

rejections_saved = spark.table(REJECTIONS_TABLE).orderBy("rule", "return_id")
display(rejections_saved)

# Small synthetic export for the public repo: drop taxpayer_id (need-to-know) and checked_at (changes every run).
# Both stay in the governed REJECTIONS_TABLE. Cast to string so pandas keeps integer amounts as written.
(REPO_ROOT / "outputs").mkdir(exist_ok=True)
rejections_export = rejections_saved.drop("taxpayer_id", "checked_at").orderBy("rule", "return_id")
rejections_export.select([F.col(c).cast("string") for c in rejections_export.columns]) \
    .toPandas().to_csv(REPO_ROOT / "outputs/initial_rejected_records.csv", index=False)

# The initial batch must fail both required rules
failed_rules = {r["rule"] for r in initial_summary.where("result = 'FAIL'").collect()}
assert {"completeness", "uniqueness"} <= failed_rules, f"Expected initial batch to fail completeness and uniqueness, got {failed_rules}"
print(f"Initial batch REJECTED; failed rules: {sorted(failed_rules)}; {rejections_saved.count()} violation rows saved to {REJECTIONS_TABLE}")

# COMMAND ----------

# DBTITLE 1,2c. Gate blocks the initial batch before feature output
# The same gate used by step 3 must refuse the initial batch, so no features can be built from it
try:
    gate_batch(INITIAL_TABLE)
    raise AssertionError("Gate unexpectedly accepted the initial batch")
except BatchRejected as err:
    initial_rejection_message = str(err)
    print("Gate refused initial batch:", initial_rejection_message)

# COMMAND ----------

# DBTITLE 1,2d. Assess the corrected batch with the same rules
# Same rules, same functions, applied to one pinned snapshot of the replacement delivery
corrected_version, corrected_df = read_snapshot(CORRECTED_TABLE)
corrected_violations = find_violations(CORRECTED_TABLE, corrected_df)
corrected_summary = summarize_checks(CORRECTED_TABLE, corrected_version, corrected_df, corrected_violations)
display(corrected_summary)
display(corrected_violations)  # expected empty

# Persist both batches' check results as evidence
initial_summary.unionByName(corrected_summary).withColumn("checked_at", F.current_timestamp()) \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(CHECK_RESULTS_TABLE)
spark.table(CHECK_RESULTS_TABLE).drop("checked_at").orderBy("source_table", "rule").toPandas() \
    .to_csv(REPO_ROOT / "outputs/batch_check_results.csv", index=False)

# Gate: raises BatchRejected (stopping the run) if the corrected batch breaks any rule
# accepted_returns is pinned to accepted_version, so step 3 reads exactly the data that was checked
accepted_version, accepted_returns = gate_batch(CORRECTED_TABLE)
assert accepted_version == corrected_version, "Corrected table changed between assessment and gate"
print(f"Corrected batch ACCEPTED: version {accepted_version}, {accepted_returns.count()} returns, all rules PASS")

# COMMAND ----------

# DBTITLE 1,2e. Read back saved rejection and check evidence
# Read the saved tables back to confirm the evidence persisted as intended
saved = spark.table(REJECTIONS_TABLE)
assert saved.where("rule = 'completeness' AND return_id = 'R02' AND income IS NULL").count() == 1
assert saved.where("rule = 'uniqueness' AND return_id = 'R01'").count() == 2
assert saved.where("rule = 'tax_year_in_scope'").count() == 0
assert spark.table(CHECK_RESULTS_TABLE).count() == 2 * len(RULES)  # every rule recorded for both batches
display(spark.table(CHECK_RESULTS_TABLE).orderBy("source_table", "rule"))

# COMMAND ----------

# DBTITLE 1,2f. Batch decision and next action
# MAGIC %md
# MAGIC ### Batch decision
# MAGIC Both batches go through the same four rules (cells 2a–2d), each checked against one pinned Delta version of the table (`source_version`). The initial batch fails, so its records and reasons are kept before any feature output is written.
# MAGIC
# MAGIC | Batch | completeness | uniqueness | nonnegative_amounts | tax_year_in_scope | Decision |
# MAGIC | --- | --- | --- | --- | --- | --- |
# MAGIC | `returns_initial` (13 rows) | FAIL: 1 row | FAIL: 2 rows | PASS | PASS | **Rejected** |
# MAGIC | `returns_corrected` (12 rows) | PASS | PASS | PASS | PASS | **Accepted** for the final run |
# MAGIC
# MAGIC **Why the initial batch fails (actual records):**
# MAGIC * **Completeness:** R02 (filer F1, 2022) has a null `income` (deduction 33). Without income, `deduction_to_income` can't be computed. Setting the income to zero would invent a valid-looking zero-income return, so it isn't allowed.
# MAGIC * **Uniqueness:** R01 (F0, 2022, income 100, deduction 20) appears twice. Both copies are kept as evidence. Keeping only one would hide a delivery defect, and keeping both would give the return two feature rows.
# MAGIC
# MAGIC **Where the evidence is kept:** the table `initial_batch_rejections` in `OUTPUT_SCHEMA` (rule, reason, full original record, `source_version` and `checked_at`), and `outputs/initial_rejected_records.csv` (same records without `taxpayer_id` and `checked_at`, because the repository is public and the file should not change between runs). Per-rule results for both batches are in the table `batch_check_results` and `outputs/batch_check_results.csv`. Cell 2c shows that `gate_batch` raises `BatchRejected` for the initial batch.
# MAGIC
# MAGIC **Next action:** step 3 builds features only from `accepted_returns`, which `gate_batch(CORRECTED_TABLE)` returns in cell 2d, pinned to the checked `accepted_version`. If a later delivery breaks a rule, the gate raises an error and the run stops before anything is written. The fault in the initial delivery belongs with the data owner. It is not repaired in this notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Deliver a checked feature table
# MAGIC Implement the task's data contract and verify the saved result by reading it back.

# COMMAND ----------

# DBTITLE 1,3a. Build and save return_features from the accepted batch
FEATURES_TABLE = f"{OUTPUT_SCHEMA}.return_features"
FEATURE_COLUMNS = ["return_id", "tax_year", "deduction_to_income"]

# Built only from accepted_returns: the corrected batch at the version the gate checked in 2d.
# Ratio = deduction / income; null when income = 0 (the return is kept, the ratio is undefined).
# taxpayer_id, income and deduction are not selected (need-to-know).
features_df = accepted_returns.select(
    "return_id",
    "tax_year",
    F.when(F.col("income") == 0, F.lit(None).cast("double"))
     .otherwise(F.col("deduction").cast("double") / F.col("income").cast("double"))
     .alias("deduction_to_income"),
)
assert features_df.columns == FEATURE_COLUMNS, features_df.columns

# Overwrite (not append) so a rerun replaces the table instead of adding copies
features_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(FEATURES_TABLE)
print(f"Saved {FEATURES_TABLE} from {CORRECTED_TABLE} version {accepted_version}")

# COMMAND ----------

# DBTITLE 1,3b. Read back return_features and verify against the source
import math

# Read the SAVED table back (not the in-memory DataFrame)
saved_features = spark.table(FEATURES_TABLE)

# 1. Exact columns and types; no filer identifier or raw amounts
assert saved_features.columns == FEATURE_COLUMNS, saved_features.columns
assert dict(saved_features.dtypes) == {"return_id": "string", "tax_year": "int", "deduction_to_income": "double"}, saved_features.dtypes

# 2. Every accepted return exactly once (same ID set as the checked source, no extras, no copies)
saved_rows = {r["return_id"]: r for r in saved_features.collect()}
source_rows = {r["return_id"]: r for r in accepted_returns.collect()}
assert saved_features.count() == len(saved_rows) == len(source_rows), "row count / uniqueness mismatch"
assert set(saved_rows) == set(source_rows), "return_id sets differ"

# 3. Independent recomputation in plain Python from the source amounts, row by row
for rid, src in source_rows.items():
    got = saved_rows[rid]
    assert got["tax_year"] == src["tax_year"], rid
    if src["income"] == 0:
        assert got["deduction_to_income"] is None, f"{rid}: zero income must give a null ratio"
    else:
        assert math.isclose(got["deduction_to_income"], src["deduction"] / src["income"], rel_tol=1e-12), rid

# 4. Spot check a nonzero case against a value derived by hand from the source row
r01_src = source_rows["R01"]
expected_r01 = r01_src["deduction"] / r01_src["income"]
assert (r01_src["income"], r01_src["deduction"]) == (100, 20) and expected_r01 == 0.2
assert math.isclose(saved_rows["R01"]["deduction_to_income"], 0.2, rel_tol=1e-12)

# 5. Zero-income returns are kept with a null ratio; no other nulls
zero_income_ids = sorted(rid for rid, s in source_rows.items() if s["income"] == 0)
null_ratio_ids = sorted(rid for rid, r in saved_rows.items() if r["deduction_to_income"] is None)
assert zero_income_ids == null_ratio_ids and len(zero_income_ids) > 0, (zero_income_ids, null_ratio_ids)

print({
    "rows": len(saved_rows),
    "R01 expected (20/100)": expected_r01,
    "R01 saved": saved_rows["R01"]["deduction_to_income"],
    "zero-income returns with null ratio": null_ratio_ids,
})
display(saved_features.orderBy("tax_year", "return_id"))

# COMMAND ----------

# DBTITLE 1,3c. Feature table contract and verification
# MAGIC %md
# MAGIC ### Feature table: `return_features`
# MAGIC * **Source:** `accepted_returns`, which is `returns_corrected` at the Delta version the gate checked in 2d (version 2 in this run). The rejected initial batch is never read here.
# MAGIC * **Columns:** exactly `return_id` (string), `tax_year` (int) and `deduction_to_income` (double). `taxpayer_id`, `income` and `deduction` are left out under the need-to-know rule.
# MAGIC * **Ratio:** `deduction_to_income = deduction / income`. When income is 0, the ratio is null and the return stays in the table.
# MAGIC * **Writes:** each run replaces the table rather than adding to it, so a rerun doesn't leave extra copies.
# MAGIC
# MAGIC **What cell 3b checks on the saved table (read back, not the in-memory DataFrame):**
# MAGIC * The columns and types are exactly as listed above.
# MAGIC * There are 12 rows and 12 distinct `return_id`s, the same ID set as the accepted source, with no extras or copies.
# MAGIC * Every ratio matches `deduction / income` recomputed in plain Python from the source amounts, and every `tax_year` matches.
# MAGIC * **Nonzero case:** R01 has income 100 and deduction 20 in the source, so the expected ratio is 20 / 100 = 0.2. The saved value is 0.2.
# MAGIC * **Zero-income case:** R03 (2022) and R09 (2024) are both present with a null ratio. They are the only nulls.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Make the handoff usable
# MAGIC Save the required year tables. Keep references and IDs-and-values evidence for two successful runs.
# MAGIC Write SUBMISSION.md and the small exports inside this Databricks Git folder.

# COMMAND ----------

# DBTITLE 1,4a. Save year split tables from the saved features
SPLIT_TABLES = {
    2022: f"{OUTPUT_SCHEMA}.features_train",
    2023: f"{OUTPUT_SCHEMA}.features_validation",
    2024: f"{OUTPUT_SCHEMA}.features_test",
}
assert set(SPLIT_TABLES) == set(TAX_YEARS), "every in-scope year needs exactly one split table"

# Splits are read from the SAVED feature table, not the in-memory DataFrame.
# Overwrite (not append) so a rerun replaces each table instead of adding copies.
features_saved = spark.table(FEATURES_TABLE)
for year, table in SPLIT_TABLES.items():
    (features_saved.where(F.col("tax_year") == year)
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table))
    print(f"Saved {table} (tax_year {year})")

# COMMAND ----------

# DBTITLE 1,4b. Read back splits: year assignment and exactly-once coverage
from functools import reduce

split_frames = []
for year, table in SPLIT_TABLES.items():
    t = spark.table(table)
    assert t.columns == FEATURE_COLUMNS, (table, t.columns)
    wrong_year = t.where(~F.col("tax_year").eqNullSafe(F.lit(year))).count()
    assert wrong_year == 0, f"{table}: {wrong_year} rows not in tax_year {year}"
    split_frames.append(t.withColumn("split_table", F.lit(table)))
all_splits = reduce(lambda a, b: a.unionByName(b), split_frames)

# Each return_id appears in exactly one split, once
multi = all_splits.groupBy("return_id").count().where("count > 1").count()
assert multi == 0, f"{multi} return_ids appear more than once across splits"

# Splits together hold exactly the saved feature rows: same IDs and values, nothing missing or extra
split_values = all_splits.drop("split_table")
missing_from_splits = features_saved.exceptAll(split_values).count()
extra_in_splits = split_values.exceptAll(features_saved).count()
assert missing_from_splits == 0 and extra_in_splits == 0, (missing_from_splits, extra_in_splits)
assert all_splits.count() == features_saved.count() == accepted_returns.count()

display(all_splits.groupBy("split_table", "tax_year").agg(
    F.count("*").alias("rows"),
    F.countDistinct("return_id").alias("distinct_return_ids"),
    F.sum(F.col("deduction_to_income").isNull().cast("int")).alias("null_ratio_rows"),
).orderBy("tax_year"))

# COMMAND ----------

# DBTITLE 1,4c. Record this run's IDs and values as evidence
import uuid
from datetime import datetime, timezone

RUN_SNAPSHOTS_TABLE = f"{OUTPUT_SCHEMA}.run_snapshots"


def _ctx_value(getter):
    try:
        opt = getter()
        return opt.get() if opt.isDefined() else None
    except Exception:
        return None


_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
NOTEBOOK_ID = _ctx_value(_ctx.notebookId)
_host = _ctx_value(_ctx.browserHostName) or spark.conf.get("spark.databricks.workspaceUrl", None)
NOTEBOOK_URL = f"https://{_host}/editor/notebooks/{NOTEBOOK_ID}" if _host and NOTEBOOK_ID else f"notebook id {NOTEBOOK_ID}"
RUN_ID = uuid.uuid4().hex[:12]
RUN_AT = datetime.now(timezone.utc).isoformat(timespec="seconds")

# Reached only after every check in steps 2-4b passed. Append is intended here:
# this evidence table keeps one snapshot per complete run (the output tables are overwritten).
snapshot = reduce(lambda a, b: a.unionByName(b), [
    spark.table(t).withColumn("table_name", F.lit(t.split(".")[-1]))
    for t in [FEATURES_TABLE, *SPLIT_TABLES.values()]
]).select(
    F.lit(RUN_ID).alias("run_id"), F.lit(RUN_AT).alias("run_at"), F.lit(NOTEBOOK_URL).alias("notebook_url"),
    F.lit(int(accepted_version)).alias("source_version"), "table_name", *FEATURE_COLUMNS,
)
snapshot.write.mode("append").saveAsTable(RUN_SNAPSHOTS_TABLE)
print({"run_id": RUN_ID, "run_at_utc": RUN_AT, "notebook_url": NOTEBOOK_URL,
       "rows_recorded": spark.table(RUN_SNAPSHOTS_TABLE).where(F.col("run_id") == RUN_ID).count()})

# COMMAND ----------

# DBTITLE 1,4d. Compare the last two complete runs (IDs and values)
import json

snap = spark.table(RUN_SNAPSHOTS_TABLE)

# No run may contain a copied row in any table
dup_in_run = snap.groupBy("run_id", "table_name", "return_id").count().where("count > 1").count()
assert dup_in_run == 0, f"{dup_in_run} duplicated (run, table, return_id) rows"

runs = (snap.groupBy("run_id", "run_at", "notebook_url", "source_version")
            .agg(F.count("*").alias("rows")).orderBy(F.col("run_at").desc()).limit(2).collect())
run_log = [r.asDict() for r in reversed(runs)]  # oldest first
(REPO_ROOT / "outputs/run_log.json").write_text(json.dumps(run_log, indent=2) + "\n", encoding="utf-8")

if len(runs) < 2:
    comparison_result = None
    print("Only one complete run recorded so far. Run the whole notebook again to produce the comparison.")
else:
    run_a, run_b = run_log[0]["run_id"], run_log[1]["run_id"]
    side = lambda rid, sfx: snap.where(F.col("run_id") == rid).select(
        "table_name", "return_id",
        F.col("tax_year").alias(f"tax_year_{sfx}"), F.col("deduction_to_income").alias(f"ratio_{sfx}"))
    comparison = (
        side(run_a, "run1").join(side(run_b, "run2"), ["table_name", "return_id"], "full_outer")
        .withColumn("status",
            F.when(F.col("tax_year_run1").isNull() | F.col("tax_year_run2").isNull(), "missing_in_one_run")
             .when(~F.col("tax_year_run1").eqNullSafe(F.col("tax_year_run2"))
                   | ~F.col("ratio_run1").eqNullSafe(F.col("ratio_run2")), "value_differs")
             .otherwise("identical"))
        .orderBy("table_name", "return_id")
    )
    comparison.toPandas().to_csv(REPO_ROOT / "outputs/run_comparison.csv", index=False)
    comparison_result = {r["status"]: r["count"] for r in comparison.groupBy("status").count().collect()}
    display(comparison.groupBy("table_name", "status").count().orderBy("table_name"))
    assert set(comparison_result) == {"identical"}, comparison_result
    print(f"Runs {run_a} and {run_b}: {comparison_result['identical']} rows identical across "
          f"return_features and the three split tables; 0 missing or different")

display(spark.createDataFrame(run_log))

# COMMAND ----------

# DBTITLE 1,4e. Export final tables and run snapshots
# Small synthetic exports of the saved tables (read back from Catalog, sorted for stable diffs)
for t in [FEATURES_TABLE, *SPLIT_TABLES.values()]:
    name = t.split(".")[-1]
    spark.table(t).orderBy("return_id").toPandas().to_csv(REPO_ROOT / f"outputs/{name}.csv", index=False)

# IDs and values for the two runs that were compared
kept_runs = [r["run_id"] for r in run_log]
(snap.where(F.col("run_id").isin(kept_runs))
     .orderBy("run_at", "table_name", "return_id")
     .toPandas().to_csv(REPO_ROOT / "outputs/run_snapshots.csv", index=False))

print(sorted(p.name for p in (REPO_ROOT / "outputs").iterdir()))

# COMMAND ----------

# DBTITLE 1,4f. Write SUBMISSION.md handoff
def _run_line(r):
    return f"| {r['run_id']} | {r['run_at']} | {r['notebook_url']} | {r['source_version']} | {r['rows']} |"

split_counts = {t.split('.')[-1]: spark.table(t).count() for t in SPLIT_TABLES.values()}

# Initial rejection reference, read from the saved rejection evidence (re-made in every complete run)
_session_tz = spark.conf.get("spark.sql.session.timeZone")
_rej = spark.table(REJECTIONS_TABLE).agg(
    F.date_format(F.max("checked_at"), "yyyy-MM-dd HH:mm:ss").alias("checked_at"),
    F.max("source_version").alias("source_version"),
    F.count("*").alias("rows"),
).first()
rejection_ref = (
    f"rejected at {_rej['checked_at']} ({_session_tz}) in run {RUN_ID}, notebook {NOTEBOOK_URL}; "
    f"`{INITIAL_TABLE}` version {_rej['source_version']}, {_rej['rows']} affected record rows "
    f"saved in `{REJECTIONS_TABLE}` (the rejection is repeated with the same rules in every complete run)"
)
comparison_text = (
    f"All {comparison_result['identical']} (table, return_id) rows are identical between the two runs "
    "(status `identical`; 0 `missing_in_one_run`, 0 `value_differs`)."
    if comparison_result else "Second complete run not yet recorded; comparison pending."
)

submission = f"""# My dataset submission

This file was generated by cell 4f of `notebook.py` from the saved tables. README.md is still the course navigation page.

## Inputs and outputs

**Inputs (read-only)**
* `{INITIAL_TABLE}`: initial delivery, rejected.
* `{CORRECTED_TABLE}`: replacement delivery, accepted at Delta version {accepted_version}.
* Lineage view `{LINEAGE_VIEW}`: both deliveries were built by SQL from `{INPUT_SCHEMA}.parsed_source`.

**One row** is one tax return. `return_id` identifies the return and must occur exactly once. `taxpayer_id` identifies the filer, who can have returns in several years.

**Outputs**
* `{FEATURES_TABLE}`: `return_id`, `tax_year`, `deduction_to_income`. {features_saved.count()} rows.
* `{SPLIT_TABLES[2022]}` (2022): the same columns, {split_counts['features_train']} rows.
* `{SPLIT_TABLES[2023]}` (2023): the same columns, {split_counts['features_validation']} rows.
* `{SPLIT_TABLES[2024]}` (2024): the same columns, {split_counts['features_test']} rows.
* Evidence tables: `{REJECTIONS_TABLE}`, `{CHECK_RESULTS_TABLE}`, `{RUN_SNAPSHOTS_TABLE}`.

**Ratio:** `deduction_to_income = deduction / income`. It is null when income = 0, and the return is kept. `taxpayer_id`, `income` and `deduction` are excluded under the need-to-know rule.

## Checks and decisions

* **Source references (step 1):** Catalog table descriptions, the lineage view, five sample rows per batch and a full-batch profile: returns by year, missing required values, duplicate IDs and zero-income records.
* **Rules (cell 2a):** applied to one pinned Delta version of each batch.
  * completeness: `return_id`, `tax_year`, `income` and `deduction` are all non-null
  * uniqueness: each `return_id` occurs once
  * nonnegative amounts
  * tax_year in 2022-2024
* **Initial rejection:** the completeness and uniqueness rules failed.
  * Rejection reference: {rejection_ref}.
  * R02 (2022) has a null `income`.
  * R01 (2022) appears twice.
  * Affected records and reasons are in `initial_batch_rejections` and `outputs/initial_rejected_records.csv`. Per-rule results are in `outputs/batch_check_results.csv`.
  * The gate raises `BatchRejected` for this batch (cell 2c), so no features are built from it. No record was dropped, deduplicated or zero-filled.
* **Replacement:** `returns_corrected` passes all four rules: 12 rows, 12 distinct IDs.
* **Nonzero expected value:** in the source, R01 has income 100 and deduction 20, so the expected ratio is 20 / 100 = 0.2. The saved value is 0.2. Every other ratio was also recomputed in plain Python from the source amounts and matched.
* **Saved-table readback (cell 3b):** the columns and types are exact. The table has 12 rows, each accepted return once. The zero-income returns R03 (2022) and R09 (2024) are present with a null ratio and are the only nulls.

## Two successful runs

| Run ID | Execution timestamp (UTC) | Notebook URL | Source version | Rows recorded |
| --- | --- | --- | --- | --- |
{chr(10).join(_run_line(r) for r in run_log)}

Each complete run ends by appending the return IDs and values of `return_features` and the three split tables to `run_snapshots` (cell 4c). This only happens after every check has passed. Cell 4d compares the last two runs with a full outer join on (table, return_id), using null-safe equality on `tax_year` and `deduction_to_income`. {comparison_text}

**Evidence files:** `outputs/run_snapshots.csv` (both runs' IDs and values), `outputs/run_comparison.csv` (row-by-row status) and `outputs/run_log.json`.

**Year assignment and exactly-once coverage (cell 4b):**
* Each split contains only its year.
* No `return_id` appears in more than one split, or more than once.
* The splits together equal `return_features` exactly (`exceptAll` in both directions is empty).

**No appended copies:** all output tables are written in overwrite mode. Cell 4d asserts that no run has a duplicated (table, return_id) row, and both runs record the same row counts.

## Final files and limitations

**Final exports in `outputs/`:**
* `return_features.csv`, `features_train.csv`, `features_validation.csv`, `features_test.csv`
* `initial_rejected_records.csv`, `batch_check_results.csv`
* `run_snapshots.csv`, `run_comparison.csv`, `run_log.json`

**Practice file (not part of the task):** `connection-practice.json`.

**Known CI result:** the starter tests were run on 2026-11-20 against an exported SOURCE copy of this notebook saved as `notebook.py`, using `python -m unittest discover -s tests -v` (the same command as `.github/workflows/tests.yml`). 11 of 12 tests pass.
* `test_browser_scaffold_uses_the_same_authenticated_schema` fails. It executes the whole `notebook.py` with a stub `spark` that only supports `spark.sql(...).first()`.
* It first stops at `NameError: name 'pwd' is not defined` (cell 2).
* Run cell by cell against the same stub, the practice cell (`.asDict()`), the setup cell (`dbutils`) and every task cell that reads or writes tables would also fail.
* The setup cell's schema calculation itself, which is what the test checks, produces an output schema name under the stub.
* The failure therefore comes from the implemented notebook needing a real Spark session. The data results are not affected.

**Not verified:**
* The Catalog UI Lineage tab. The supplied lineage view was used instead, and it does not claim a UI check.
* The contents of `parsed_source` beyond its Catalog description.
* Model leakage. The same filer (for example F0) has returns in the train, validation and test years. Labels and prediction-time availability are left to later modelling work.
* Whether the GitHub Actions run on push shows the same single failure. The tests were run in Databricks, not on GitHub's runner.
* The final GitHub commit contents. They are checked after Commit & Push, and the commit SHA is reported in Classroom, not here.
"""
(REPO_ROOT / "SUBMISSION.md").write_text(submission, encoding="utf-8")
print(submission)