# Handoff — next session

Written 2026-07-30. Three tasks, ordered by value. Task 1 is a one-line fix with a
verified patch. Task 2 is a live production failure. Task 3 is cleanup.

## Context you need first

**Run dbt with dbt-core, not the `dbt` on PATH.** The global `dbt` is dbt Fusion and
**cannot parse this project** — it rejects every `accepted_values` block with 22
errors. The project standardized on dbt-core (pinned in
`dbt/supermarket_dwh/requirements.txt`: `dbt-core==1.12.0`, `dbt-bigquery==1.12.0`).
The two engines disagree irreconcilably on generic-test YAML: Fusion requires args
nested under `arguments:`, dbt-core requires them flat. **The yml uses the flat form
on purpose — do not "fix" it back.**

As of this writing there is no permanent local dbt-core install. `pipx install
dbt-bigquery` does not work (it is a library with no executables — the `dbt` binary
belongs to its dependency `dbt-core`), and `~/.local/bin/dbt` is already taken by
Fusion. Untested suggestion:

```bash
pipx install dbt-core==1.12.0 && pipx inject dbt-core dbt-bigquery==1.12.0
which -a dbt   # confirm Fusion isn't shadowing it
```

**Warehouse is incremental.** Models in the `scrape_date` lineage use
`insert_overwrite` with a 3-day lookback, partitioned on `scrape_date`, clustered on
`supermarket`. Any change to model logic or a seed only affects newly built
partitions — **you must `dbt build --full-refresh`** to apply it to history. Alfonso
does this deliberately on every change (~1-2x/month) and is fine with the cost; do
not propose a scheduled periodic full-refresh.

**Branch state:** 3 unpushed commits on `dwh-case-size-and-incremental` (`369b40a`,
`952f549`, `d2a6aa1`). Working tree clean. Not merged to `main`.

**Verified green:** `dbt build --full-refresh` → 178/178 pass, 0 errors, 0 skips.

---

## Task 1 — DIA scraper truncates the unit off every unit price

**Impact:** 159,468 rows (~3.6% of the warehouse) reach gold as
`case_validation_status = 'unavailable'` and are filtered out of product search.
This is the single largest remaining data gap. It is also the entire remaining
`jabón de manos` gap (21 of 43 rows) that prompted the original investigation.

**Root cause**, `scrapers/dia/common/parsing.py:13`:

```python
PPU_RE = re.compile(
    r"(\d{1,3}(?:[.,]\d{3})*[.,]\d{2}\s*€\s*(?:/|por)\s*[\wáéíóúñ%]+)",
    flags=re.IGNORECASE,
)
```

For `"7,30 € / 100 g"` the trailing `[\wáéíóúñ%]+` matches `100` and stops at the
space, so the captured value is `"7,30 € / 100"` — the unit is discarded. Per-litre
and per-unit forms (`1,29 €/l`) are unaffected, which is why this went unnoticed.

**Fix** — allow an optional numeric reference quantity before the unit:

```python
PPU_RE = re.compile(
    r"(\d{1,3}(?:[.,]\d{3})*[.,]\d{2}\s*€\s*(?:/|por)\s*(?:\d+(?:[.,]\d+)?\s*)?[\wáéíóúñ%]+)",
    flags=re.IGNORECASE,
)
```

Verified against all five observed forms:

| input | current | fixed |
|---|---|---|
| `7,30 € / 100 g` | `7,30 € / 100` | `7,30 € / 100 g` |
| `0,57 € / 100 ml` | `0,57 € / 100` | `0,57 € / 100 ml` |
| `1,29 €/l` | `1,29 €/l` | `1,29 €/l` |
| `2,70 €/ud` | `2,70 €/ud` | `2,70 €/ud` |
| `4,90 € por docena` | `4,90 € por docena` | `4,90 € por docena` |

The warehouse side already handles `€/100 g` correctly — `int_products_prices_parsed`
parses the reference quantity and divides it out, with
`assert_unit_price_per_reference_quantity_scaled` pinning the arithmetic. So once the
scraper emits the unit, these rows resolve with no dbt change.

**Caveat:** this only fixes rows scraped *after* the fix ships. Historical DIA rows
stay `unavailable` — the unit was never captured, and inferring g vs ml from the
product name would be a guess. Do not attempt to backfill it in dbt.

**Verify after deploying:**

```sql
select count(*) from `lab-spanish-smarkts-scraper.dwh_silver_dev.silver_product_standardized`
where supermarket='dia' and scrape_date = current_date()
  and unit_base_unit_std is null and price_per_unit_raw != ''
```

Should drop to ~0 for new dates.

---

## Task 2 — the scheduled dbt job is failing every morning

The Cloud Run job `dbt-runner-dev` and scheduler `dbt-runner-daily-dev` are both
deployed and enabled. The manual execution succeeded. **The first scheduled run
failed**, and will keep failing daily.

```
gcloud run jobs executions list --job=dbt-runner-dev \
  --region=europe-southwest1 --project=lab-spanish-smarkts-scraper
# X  dbt-runner-dev-4scrt  0/1  2026-07-30 05:30:01 UTC
```

**This is the readiness check doing its job, not a bug.** At 07:30 Madrid only
mercadona had loaded; dia reported `latest_date: 2026-07-29, target_rows: 0`. The
container exited 1 rather than building a partial warehouse. Correct behavior,
wrong schedule.

**What to do:** find when all four Bronze tables actually land, then move the
schedule after that. The scrapers trigger at 06:00 Madrid but clearly do not all
finish by 07:30.

```sql
-- per-supermarket load completion time over the last 2 weeks
select table_name, max(last_modified_time)
from `lab-spanish-smarkts-scraper.dwh_bronze_dev`.__TABLES__
```

Then:

```bash
gcloud scheduler jobs update http dbt-runner-daily-dev \
  --location=europe-west1 --schedule="0 10 * * *" --time-zone="Europe/Madrid" \
  --project=lab-spanish-smarkts-scraper
```

Consider instead making the job resilient rather than guessing a time: Cloud Run
`--max-retries` with a backoff, or an Eventarc trigger on the Bronze load finishing.
Alfonso has an `ingestion/bq_event_loader` already, so the event-driven path may be
short.

**Watch out:** `gcloud`'s default project is `cyber-analyzer-490508`, **not** the
warehouse project — always pass `--project` explicitly. And Cloud Scheduler is not
available in `europe-southwest1`; the scheduler lives in `europe-west1` and calls
across regions into the job. Full runbook: `docs/deploy_dbt_runner.md`.

---

## Task 3 — cleanup

- **Push / merge the branch.** 3 commits sit unpushed on
  `dwh-case-size-and-incremental`.
- **Alcampo scraper leaks accessibility text** into `subcategory`, e.g.
  `"mejillones (se abre en una ventana nueva)"` — 13,757 rows across 22
  subcategories. dbt strips it defensively (`macros/strip_nav_noise.sql`) and a test
  guards against rules being written against the junk
  (`assert_standard_rules_have_no_nav_noise`), so this is not urgent — but the
  scraper should not emit it. In `scrapers/al_campo/`.
- **316 `incompatible_units` rows are real conflicts, not bugs** — cat litter named
  `6L` priced `€/kg`, a `30 g` batido priced `€/L`, esparadrapo in `ud` priced `€/m`.
  The model correctly refuses to guess a density. Only worth touching if you want
  them surfaced as a data-quality report to the supermarkets' listings.

---

## Current numbers (2026-07-30, `fact_products_today`, 30,434 rows)

| `case_validation_status` | rows |
|---|---|
| `matched_unit_price` | 21,807 |
| `derived_from_unit_price` | 6,447 |
| `unit_price_mismatch` | 1,208 |
| `unavailable` | 656 |
| `incompatible_units` | 316 |

Trusted (matched + derived) = 28,254. Task 1 is what moves `unavailable` further.
