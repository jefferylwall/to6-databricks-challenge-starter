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

# Your implementation and verification here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Make the handoff usable
# MAGIC Save the required year tables. Keep references and IDs-and-values evidence for two successful runs.
# MAGIC Write SUBMISSION.md and the small exports inside this Databricks Git folder.

# COMMAND ----------

# Your implementation, rerun comparison and exports here.