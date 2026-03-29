# Testing Guide

This document is for a tester validating the current Anti-Manipulation prototype.

## Goal

Verify that the demo flow works consistently across all supported dataset families and that the UI behavior matches the intended paper demo flow.

## Setup

From [Anti-Manipulation](/Users/aryal/Desktop/QueryGuard):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

Open [http://127.0.0.1:5000](http://127.0.0.1:5000).

## Smoke Test

### 1. Dataset dropdown

Expected:

- The page loads without errors.
- The dropdown lists all bundled demo sources.
- Clicking `Load` changes the query builder controls for the selected dataset.

### 2. Query builder

Expected:

- Categorical filters show checkboxes or search+add depending on cardinality.
- Numeric filters show sliders.
- `Featured` and `Custom` ranking modes both render correctly.
- `Top-K` is editable in the top summary panel.

### 3. Run Query

Expected:

- Clicking `Run Query` updates the result table.
- The ranking summary updates.
- The analysis panel stays empty until `Find Influential` or `Improve Query` is clicked.

### 4. Analyze Trustworthiness

Expected:

- Clicking `Analyze Trustworthiness` highlights suspicious rows in red.
- No separate popup is shown.
- The current result order remains visible.

### 5. Find Influential

Expected:

- Clicking `Find Influential` creates a generated query card.
- `Submit Query` appears inside that card.
- The result table does not change until `Submit Query` is clicked.

### 6. Improve Query

Expected:

- Clicking `Improve Query` updates the generated query card.
- The result table still does not change until `Submit Query` is clicked.

### 7. Submit Query

Expected:

- Clicking `Submit Query` updates the result table.
- The summary changes to indicate influential/improved behavior.

### 8. Save Result

Expected:

- Clicking `Save Result` adds a card in the right panel.
- The saved card includes:
  - selections summary
  - ranking summary
  - top-k
  - shown row count
  - saved time
  - tuple ids
- `Show More` reveals the exact saved details.
- Saved cards survive a page refresh.

### 9. Hide Saved

Expected:

- Clicking `Hide Saved` collapses the right panel.
- The result table expands to full width.
- Clicking again restores the right panel.

## Recommended Paper Walkthrough Test

Use `Amazon Headphones Demo`.

Steps:

1. Load `Amazon Headphones Demo`.
2. Keep `category = headphones`.
3. Set `price_max = 20`.
4. Set `rating_min = 4`.
5. Rank by `rating desc`.
6. Keep `Top-K = 16`.
7. Click `Run Query`.
8. Click `Analyze Trustworthiness`.
9. Click `Find Influential`.
10. Click `Submit Query`.
11. Click `Improve Query`.
12. Click `Submit Query`.
13. Click `Save Result`.

Expected:

- suspicious rows are highlighted during analysis,
- generated query card appears,
- submitting the generated query changes the visible result,
- saved result card appears and persists.

## Cross-Dataset Checks

### Amazon

Verify:

- `category`, `brand`, and `best_seller` controls appear
- `rating`, `price`, and `sales_last_month` can be used for ranking

### PriceRunner

Verify:

- `product_category`, `seller`, and `product_model` controls appear
- ranking attributes match available numeric/canonical fields

### Flights

Verify:

- `airline`, `source`, `destination`, and `price` controls appear
- ranking attributes include `price`, `days_until_departure`, and `duration`

## Known Prototype Limitations

The tester should treat these as expected unless the project scope changes:

- the research algorithms are placeholders
- featured ranking is demo-only
- saved results are browser-local, not server-persisted
- there is only one active backend session at a time

## Regression Checks After Backend Changes

If a developer replaces the placeholder backend, retest:

- row highlighting after `/api/analyze`
- generated query card contents
- `Submit Query` behavior
- schema normalization for all four bundled sources
- saved result persistence and `Show More` details
