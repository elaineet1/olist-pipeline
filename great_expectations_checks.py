import great_expectations as gx
import warnings
warnings.filterwarnings("ignore")

# ── Setup ────────────────────────────────────────────────────────────────────
PROJECT    = "my-project-001-487905"
DATASET    = "olist_dw"
CONNECTION = f"bigquery://{PROJECT}/{DATASET}"

context = gx.get_context(mode="ephemeral")

# ── Data Source ──────────────────────────────────────────────────────────────
datasource = context.data_sources.add_sql(
    name="bigquery_olist",
    connection_string=CONNECTION,
)

# ── Helper to run expectations on a table ───────────────────────────────────
def check_table(table_name, expectations):
    print(f"\n{'='*60}")
    print(f"Checking: {table_name}")
    print(f"{'='*60}")

    asset = datasource.add_table_asset(name=table_name, table_name=table_name)
    batch = asset.add_batch_definition_whole_table(f"{table_name}_batch")
    suite = context.suites.add(gx.ExpectationSuite(name=f"{table_name}_suite"))

    for exp in expectations:
        suite.add_expectation(exp)

    validation = context.validation_definitions.add(
        gx.ValidationDefinition(
            name=f"{table_name}_validation",
            data=batch,
            suite=suite,
        )
    )

    results = validation.run()

    passed = sum(1 for r in results.results if r.success)
    total  = len(results.results)
    print(f"Results: {passed}/{total} expectations passed")

    for r in results.results:
        status = "✅ PASS" if r.success else "❌ FAIL"
        print(f"  {status} — {r.expectation_config.type}: {dict(r.expectation_config.kwargs)}")

    return results

# ── Expectations ─────────────────────────────────────────────────────────────
all_results = {}

# fact_order_items
all_results["fact_order_items"] = check_table("fact_order_items", [
    gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id"),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="product_id"),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="seller_id"),
    gx.expectations.ExpectColumnValuesToBeBetween(column="price", min_value=0),
    gx.expectations.ExpectColumnValuesToBeBetween(column="freight_value", min_value=0),
    gx.expectations.ExpectColumnValuesToBeBetween(column="total_item_amount", min_value=0),
])

# fact_order_payments
all_results["fact_order_payments"] = check_table("fact_order_payments", [
    gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id"),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="payment_type"),
    gx.expectations.ExpectColumnValuesToBeBetween(column="payment_value", min_value=0),
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="payment_type",
        value_set=["credit_card", "boleto", "voucher", "debit_card", "not_defined"]
    ),
])

# fact_order_reviews
all_results["fact_order_reviews"] = check_table("fact_order_reviews", [
    gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id"),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="review_score"),
    gx.expectations.ExpectColumnValuesToBeBetween(column="review_score", min_value=1, max_value=5),
])

# dim_geolocation
all_results["dim_geolocation"] = check_table("dim_geolocation", [
    gx.expectations.ExpectColumnValuesToNotBeNull(column="geolocation_zip_code_prefix"),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="geolocation_city"),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="geolocation_state"),
])

# ── Summary ───────────────────────────────────────────────────────────────────
print(f"\n{'='*60}")
print("GREAT EXPECTATIONS SUMMARY")
print(f"{'='*60}")
total_passed = sum(
    sum(1 for r in res.results if r.success)
    for res in all_results.values()
)
total_all = sum(len(res.results) for res in all_results.values())
print(f"Total: {total_passed}/{total_all} expectations passed across all tables")

# ── HTML Report ──────────────────────────────────────────────────────────────
print("\nGenerating HTML report...")

import json
from datetime import datetime

report_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

html_rows = ""
for table_name, res in all_results.items():
    for r in res.results:
        status = "PASS" if r.success else "FAIL"
        color = "#28a745" if r.success else "#dc3545"
        exp_type = r.expectation_config.type
        kwargs = {k: v for k, v in dict(r.expectation_config.kwargs).items() if k != "batch_id"}
        html_rows += f"""
        <tr>
            <td>{table_name}</td>
            <td>{exp_type}</td>
            <td>{kwargs}</td>
            <td style="color:{color}; font-weight:bold">{status}</td>
        </tr>"""

total_passed = sum(sum(1 for r in res.results if r.success) for res in all_results.values())
total_all = sum(len(res.results) for res in all_results.values())

html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Great Expectations Report — Olist Pipeline</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; background: #f8f9fa; }}
        h1 {{ color: #343a40; }}
        .summary {{ background: #fff; padding: 20px; border-radius: 8px; margin-bottom: 20px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        .badge {{ font-size: 1.5em; font-weight: bold; color: #28a745; }}
        table {{ width: 100%; border-collapse: collapse; background: #fff;
                 box-shadow: 0 2px 4px rgba(0,0,0,0.1); border-radius: 8px; overflow: hidden; }}
        th {{ background: #343a40; color: white; padding: 12px; text-align: left; }}
        td {{ padding: 10px 12px; border-bottom: 1px solid #dee2e6; }}
        tr:hover {{ background: #f1f3f5; }}
    </style>
</head>
<body>
    <h1>🔍 Great Expectations — Data Quality Report</h1>
    <div class="summary">
        <p><strong>Project:</strong> Olist Brazilian E-Commerce Pipeline</p>
        <p><strong>Generated:</strong> {report_time}</p>
        <p><strong>Result:</strong> <span class="badge">{total_passed}/{total_all} expectations passed ✅</span></p>
    </div>
    <table>
        <thead>
            <tr>
                <th>Table</th>
                <th>Expectation</th>
                <th>Parameters</th>
                <th>Result</th>
            </tr>
        </thead>
        <tbody>
            {html_rows}
        </tbody>
    </table>
</body>
</html>
"""

report_path = "/home/dsai/olist-pipeline/docs/ge_report.html"
with open(report_path, "w") as f:
    f.write(html)

print(f"HTML report saved to: {report_path}")