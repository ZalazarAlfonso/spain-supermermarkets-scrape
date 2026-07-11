# Plan 06: AI Shopping Agent

## Target

Build an AI agent that can answer questions about supermarket products and help decide where to buy items from a shopping list.

The first useful version should answer:

- "Where is milk cheapest today?"
- "Show me olive oil under 6 euros."
- "Which products dropped in price?"
- "Given this shopping list and these supermarkets, where should I buy each item?"

The agent should be grounded in the API and gold warehouse tables. It should not guess prices, availability, or supermarkets when the data is missing.

## Start Here

Do not start this before the API has basic product search. The agent needs tools it can call, not direct free-form access to warehouse data.

Minimum API endpoints needed first:

- `GET /products`
- `GET /categories`
- `GET /products/{source_product_id}/price-history`

Useful next endpoint:

- `POST /shopping-list/resolve` or an internal equivalent tool that maps user list items to candidate products.

## Middle Goals

### Middle Goal 1: Product Q&A Agent

Create a first agent that can answer product questions using API calls.

Example questions:

- "What is the cheapest rice in Mercadona?"
- "Do we have oat milk products?"
- "What products are available in Bebidas?"
- "What changed price recently?"

Done when:

- The agent uses product search instead of inventing answers.
- It cites the product name, supermarket, price, and date/source freshness.
- It says when no matching product is found.

### Middle Goal 2: Shopping List Parser

Convert free-text shopping lists into structured items.

Example input:

```text
2 bottles of milk
olive oil
eggs
bananas
coffee capsules
```

Expected structured output:

- item name.
- optional quantity.
- optional unit.
- optional preferences.
- optional excluded products.

Done when:

- A messy human shopping list becomes clean JSON.
- The parser keeps uncertainty instead of pretending every item is exact.

### Middle Goal 3: Product Matching

Match each shopping-list item to candidate products.

For each item, return:

- best exact matches.
- possible substitutes.
- unavailable/uncertain matches.
- supermarket.
- current price.
- unit price where available.

Done when:

- Each list item has ranked product candidates.
- The agent can explain why a match was chosen.
- Ambiguous items ask for clarification or show alternatives.

### Middle Goal 4: Supermarket Allocation

Given allowed supermarkets, decide where to buy each item.

Inputs:

- shopping list.
- allowed supermarkets.
- preference mode: cheapest, fewest stores, balanced, or preferred supermarket first.

Output:

- item-by-item recommendation.
- total estimated cost by supermarket.
- total estimated basket cost.
- items not found.
- alternatives/substitutions.

Done when:

- The agent can produce a realistic "buy this here, buy that there" answer.
- It can compare cheapest total basket against fewer-store convenience.

### Middle Goal 5: Agent API Endpoint

Expose the agent through the API.

Suggested endpoints:

- `POST /agent/ask`
- `POST /agent/shopping-list`

Done when:

- The website can send a natural language question.
- The website can send a shopping list and selected supermarkets.
- Responses include structured data plus a human-readable explanation.

### Middle Goal 6: Evaluation Set

Create a small test set of expected questions and shopping lists.

Include:

- simple product search.
- ambiguous product names.
- unavailable products.
- price comparison.
- shopping list across two supermarkets.
- shopping list across all supermarkets.

Done when:

- You can run repeatable checks before changing prompts, matching logic, or API queries.

## Build Checklist

- Add product search tools that call the API.
- Add a shopping-list parser.
- Add candidate product matching.
- Add supermarket allocation logic.
- Add answer format with both text and structured JSON.
- Add guardrails for missing/stale data.
- Add tests/evals for common product questions.
- Add website UI later: chat box, shopping list form, selected supermarket checkboxes.

## Suggested Agent Behavior

- Always use current product data from the API.
- Mention when data freshness is unknown or not from today.
- Prefer unit price for comparable products when available.
- Separate exact matches from substitutes.
- Do not claim a product is unavailable everywhere unless all selected supermarkets were checked.
- When the list item is vague, show the best options and ask a short clarification only if needed.

## Done When

- A user can ask product questions and get grounded answers.
- A user can provide a shopping list and selected supermarkets.
- The agent returns where to buy each item, estimated total cost, and unresolved items.
- The website can display the agent answer without parsing free-form text only.

## Later

- Product matching across supermarkets for equivalent items.
- Basket optimization with travel/time cost.
- User preferences like brand, organic, gluten-free, or package size.
- Saved shopping lists.
- Price alerts.
- Nutrition or allergen filters, only if reliable product data exists.
