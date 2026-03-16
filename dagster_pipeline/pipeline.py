import subprocess
from dagster import asset, AssetExecutionContext, Definitions, ScheduleDefinition, define_asset_job

# ── Asset 1: dbt build ────────────────────────────────────────────────────────
@asset(
    name="dbt_build",
    description="Runs dbt build to transform raw data into star schema and run all tests"
)
def dbt_build(context: AssetExecutionContext):
    context.log.info("Starting dbt build...")
    result = subprocess.run(
        ["dbt", "build"],
        cwd="/home/dsai/olist-pipeline/olist_dbt",
        capture_output=True,
        text=True
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt build failed:\n{result.stderr}")
    context.log.info("dbt build completed successfully")
    return result.stdout

# ── Asset 2: Great Expectations ───────────────────────────────────────────────
@asset(
    name="great_expectations_checks",
    description="Runs Great Expectations data quality checks and generates HTML report",
    deps=[dbt_build]
)
def great_expectations_checks(context: AssetExecutionContext):
    context.log.info("Running Great Expectations checks...")
    result = subprocess.run(
        ["python", "/home/dsai/olist-pipeline/great_expectations_checks.py"],
        cwd="/home/dsai/olist-pipeline",
        capture_output=True,
        text=True
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Great Expectations failed:\n{result.stderr}")
    context.log.info("Great Expectations checks completed successfully")
    return result.stdout

# ── Job: runs all assets in order ─────────────────────────────────────────────
olist_pipeline_job = define_asset_job(
    name="olist_pipeline_job",
    selection=["dbt_build", "great_expectations_checks"]
)

# ── Schedule: runs daily at 6am ───────────────────────────────────────────────
daily_schedule = ScheduleDefinition(
    job=olist_pipeline_job,
    cron_schedule="0 6 * * *",
    name="olist_daily_schedule"
)

# ── Definitions: registers everything with Dagster ────────────────────────────
defs = Definitions(
    assets=[dbt_build, great_expectations_checks],
    jobs=[olist_pipeline_job],
    schedules=[daily_schedule]
)