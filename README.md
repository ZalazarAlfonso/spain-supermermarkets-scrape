# Carrefour Supermercado Scraper

Scrapes product data from Carrefour Spain (`carrefour.es`) for the supermarket groups **Frescos**, **La Despensa**, and **Bebidas**. Output is a daily CSV with product data and category/subcategory labels.

## Files
- Orchestrator: `/Users/alfonsozalazaae/Documents/Codex/main.py`
- Script: `/Users/alfonsozalazaae/Documents/Codex/scrape_carrefour_food.py`
- Requirements: `/Users/alfonsozalazaae/Documents/Codex/requirements.txt`
- Output folder (default): `/Users/alfonsozalazaae/Documents/Codex/files`

## Output
A daily CSV is created at:
- `/Users/alfonsozalazaae/Documents/Codex/files/carrefour_supermercado_YYYY-MM-DD.csv`

Columns:
- `date`
- `product`
- `brand`
- `price`
- `price_per_unit`
- `offer`
- `category`
- `subcategory`
- `product_url`

## Category + Subcategory Rules
Main group URLs (default):
- `https://www.carrefour.es/supermercado/frescos/cat20002/c`
- `https://www.carrefour.es/supermercado/la-despensa/cat20001/c`
- `https://www.carrefour.es/supermercado/bebidas/cat20003/c`

Parsing rules:
- All group pages: find `div.nav-second-level-categories` and iterate `div.nav-second-level-categories__slide`.
- Category name source: `title` on the slide. Fallback: `<p class="nav-second-level-categories__text">`.
- Exclude any slide whose name contains `oferta` (case-insensitive).

For **Frescos** and **La Despensa**:
- Group page slide names are **categories** (e.g., `Carne`, `Pescado y Marisco`).
- Each category link is visited, and its page `nav-second-level-categories` provides **subcategories** (e.g., `Aves y Pollo`).
- Output: `category=<category name>`, `subcategory=<subcategory name>`.

For **Bebidas**:
- Group page slide names are **subcategories** (e.g., `Cerveza`, `Vinos`).
- Output: `category=Bebidas`, `subcategory=<slide name>`.

If a category page has no subcategories, the scraper falls back to:
- `category=<category name>`
- `subcategory=""`
- `category_url=<category page>`

## Install
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r /Users/alfonsozalazaae/Documents/Codex/requirements.txt
```

Optional (only if needed):
```bash
pip install playwright
playwright install
```

## Run
```bash
python /Users/alfonsozalazaae/Documents/Codex/main.py
```

Useful options:
```bash
python /Users/alfonsozalazaae/Documents/Codex/main.py \
  --max-categories 5 \
  --max-pages 2 \
  --max-products 200 \
  --sleep 1.0
```

## Environment Overrides
Override the group URLs (comma-separated):
```bash
export CARREFOUR_GROUP_URLS="https://www.carrefour.es/supermercado/frescos/cat20002/c,https://www.carrefour.es/supermercado/la-despensa/cat20001/c,https://www.carrefour.es/supermercado/bebidas/cat20003/c"
```

## Logging
The script prints progress for:
- Group category discovery
- Category and subcategory counts
- Pages scraped and pagination queue size
- Total rows and elapsed time

## Notes
- Carrefour pages may block non-browser clients. The script will fall back to Playwright rendering when needed.
- `brand` is often missing on product cards and will be blank in those cases.

## Docker
```bash
docker build -t carrefour-scraper .
docker run --rm -v \"$PWD/files:/app/files\" carrefour-scraper
```
