# Plan 04: Website

## Target

Build a simple website that makes the supermarket data usable.

The first version should help someone:

- search products.
- filter by supermarket and category.
- open a product detail page.
- see price history.
- browse recent price changes.

## Start Here

Do not start this before the API has at least:

- `GET /products`
- `GET /categories`
- `GET /products/{source_product_id}/price-history`

Once those exist, create the frontend skeleton and wire it to the API.

## Middle Goals

### Middle Goal 1: Frontend Skeleton

Pick a frontend stack.

Good default:

- Vite + React for simple app speed.
- Next.js if you want routing/server features from day one.

Done when:

- The app loads locally.
- API base URL is configurable.
- There is a basic route/page structure.

### Middle Goal 2: Product Search Page

Build the main page around useful data, not marketing.

Include:

- search input.
- supermarket filter.
- category/subcategory filter.
- product result list.
- loading state.
- empty state.

Done when:

- A user can search and filter real products from the API.

### Middle Goal 3: Product Detail Page

Add a detail page.

Include:

- product name.
- supermarket.
- brand.
- current price.
- unit price.
- category/subcategory.
- product URL.
- price history chart.

Done when:

- A user can click from search results into a product and see price history.

### Middle Goal 4: Price Changes Page

Add a page for recent price movement.

Include:

- price drops.
- price increases.
- filters for supermarket/category.

Done when:

- The website has a reason to return beyond basic product lookup.

### Middle Goal 5: Deploy

Pick a simple host.

Options:

- Vercel.
- Firebase Hosting.
- Cloud Run static server.

Done when:

- The deployed website can talk to the deployed API.

## Build Checklist

- Use the API only, never direct BigQuery access.
- Add reusable components for filters, product rows, and charts.
- Add loading, error, and empty states.
- Keep the first UI functional and clean.
- Add deployment instructions.

## Done When

- The user can search, filter, and open product detail pages.
- Price history renders from API data.
- The website is deployed or ready to deploy.

## Later

- Saved products.
- Price alerts.
- Basket comparison.
- SEO pages for categories/products.
- User accounts.
