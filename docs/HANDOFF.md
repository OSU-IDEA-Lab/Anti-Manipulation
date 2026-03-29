# Developer Handoff

This document is for the developer who will replace the current demo backend with a fuller implementation.

## What This Prototype Already Solves

- End-to-end Flask app wiring
- dataset loading and schema normalization
- dynamic frontend generation from backend metadata
- query submission flow
- trust-analysis UI behavior
- reformulation UI behavior
- saved-result comparison workflow

The app is therefore already a good shell for the real system. The main work left is algorithmic and backend-quality, not UI rethinking.

## Main Replacement Points

### 1. Replace sample algorithms

File: [algorithms.py](/Users/aryal/Desktop/QueryGuard/algorithms.py)

Current functions:

- `sample_check_influential_equilibrium`
- `sample_detect_untrustworthy_tuples`
- `sample_find_influential_query`
- `sample_improve_query`

These are placeholder contracts. The UI and Flask routes already depend on their output shapes.

Recommended rule:

- keep the response keys stable where possible,
- improve the internal logic behind them.

### 2. Replace source-bias heuristics

File: [app.py](/Users/aryal/Desktop/QueryGuard/app.py)

Current demo logic:

- `_bias_score`
- `_rank_by_biased_source`
- `_rank_featured`
- `_rank_with_case_query`

These simulate source-side manipulation and reformulation effects. They are intentionally simple and not meant to be the final implementation.

### 3. Decide whether to persist server-side

Current behavior:

- the active query session is stored in memory on the Flask side,
- saved result summaries are stored in browser local storage on the frontend.

Production options:

- session storage
- database-backed saved runs
- per-user persistence
- export/import of saved comparisons

## Current Data Contracts

### `/api/load-dataset`

Frontend expects:

- `datasetLabel`
- `datasetType`
- `description`
- `rowCount`
- `filters`
- `rankingAttributes`
- `defaults`

### `/api/query`

Frontend expects:

- `resultColumns`
- `result`
- `summary`

Each result row currently includes:

- visible fields
- `tupleID`
- internal `_rowId`

### `/api/analyze`

Frontend currently uses only:

- `flagged[].rowId`

The UI highlights result rows based on those ids.

### `/api/find-influential` and `/api/improve-query`

Frontend currently uses:

- `relativeConstraint`
- `reformulation.queryText`

### `/api/submit-query`

Frontend expects the same result shape as `/api/query`.

## Important UI Assumptions

File: [static/app.js](/Users/aryal/Desktop/QueryGuard/static/app.js)

The frontend assumes:

- one active dataset at a time,
- one active query at a time,
- one active generated reformulation at a time,
- saved results are summaries for human comparison, not backend truth.

If you change API contracts, this is the main file to update.

## Dataset Handling Notes

File: [app.py](/Users/aryal/Desktop/QueryGuard/app.py)

The loader:

1. detects dataset family from header columns,
2. resolves aliases into canonical names,
3. fills required fallback columns,
4. emits metadata for a dataset-aware UI.

If you add a new dataset family:

1. add a `DatasetConfig`,
2. add canonical field names in `DATASET_FIELDS`,
3. add aliases in `COLUMN_ALIASES`,
4. update the sample catalog if you want it visible in the dropdown.

## Paper-to-Code Mapping

This project currently maps paper concepts as follows:

- user intent ranking
  - `_rank_by_intent`
- biased source ranking
  - `_rank_by_biased_source`
- highlighted suspicious tuples
  - `sample_detect_untrustworthy_tuples`
- influential reformulation
  - `sample_find_influential_query`
- improved reformulation
  - `sample_improve_query`

The Amazon running example is preserved in:

- [sample_data/amazon_headphones.csv](/Users/aryal/Desktop/QueryGuard/sample_data/amazon_headphones.csv)

Default demo filters for that source are:

- `category = headphones`
- `price_max = 100`
- `rating_min = 4`

For a closer paper-style walkthrough, use:

- `price_max = 20`
- rank by `rating desc`
- `Top-K = 16`

## Suggested Backend Implementation Order

1. Replace trust detection first.
   Reason: the current UI already makes trust highlighting useful and easy to verify.
2. Replace influential reformulation generation.
   Reason: this is the most visible backend feature in the UI.
3. Replace improved-query logic.
4. Replace heuristic source-bias scoring with the actual model.
5. Revisit featured ranking mode.
6. Add persistence or multi-user handling if needed.

## Suggested Engineering Improvements

- move sample catalog config into a dedicated config module
- add typed response models or dataclasses for API payloads
- add unit tests for:
  - schema normalization
  - dataset type detection
  - filter application
  - ranking paths
  - algorithm response contracts
- add structured logging around API routes
- add server-side persistence for saved results if required
- disable `debug=True` for production

## Fast Orientation

If you only have a few minutes, read these files in this order:

1. [README.md](/Users/aryal/Desktop/QueryGuard/README.md)
2. [app.py](/Users/aryal/Desktop/QueryGuard/app.py)
3. [algorithms.py](/Users/aryal/Desktop/QueryGuard/algorithms.py)
4. [static/app.js](/Users/aryal/Desktop/QueryGuard/static/app.js)
