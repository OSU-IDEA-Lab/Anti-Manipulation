"""Flask demo app for the Anti-Manipulation paper prototype.

This project is intentionally split into two layers:

1. ``app.py`` owns data loading, dataset normalization, ranking flow, and API
   endpoints.
2. ``algorithms.py`` owns the paper-inspired algorithm hooks. Those functions
   currently return deterministic sample outputs so a later backend developer
   can replace them with the real research implementation without having to
   redesign the UI or request/response contracts.

The code is written to be readable first. It is not an optimized research
implementation and it does not try to reproduce every formal detail from the
paper. Instead, it provides a clean end-to-end demo skeleton that mirrors the
paper workflow:

- choose a dataset and intent,
- run the original query,
- analyze trustworthiness,
- generate an influential reformulation,
- improve that reformulation,
- submit the generated query and compare results.
"""

from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
import random

import pandas as pd
from flask import Flask, jsonify, render_template, request

from algorithms import (
    sample_check_influential_equilibrium,
    sample_detect_untrustworthy_tuples,
    sample_find_influential_query,
    sample_improve_query,
)


BASE_DIR = Path(__file__).resolve().parent
# Keep one shared default so UI and backend stay aligned on the initial visible
# window size. The paper demo frequently talks about top-k / top-16 results.
TOP_K = 16


@dataclass
class DatasetConfig:
    """Static configuration for one supported dataset family.

    Each dataset family needs:
    - which filters should appear in the UI,
    - which attributes can be used for ranking,
    - which columns should be shown in the result table,
    - which attribute the demo treats as the possible manipulation axis.
    """

    key: str
    label: str
    description: str
    filters: list[dict[str, Any]]
    ranking_attributes: list[str]
    display_columns: list[str]
    preview_columns: list[str]
    default_filters: dict[str, Any]
    default_sort: str
    default_direction: str
    bias_column: str
    bias_positive_values: list[str] = field(default_factory=list)
    bias_negative_values: list[str] = field(default_factory=list)


# These configs drive both backend behavior and the dynamic UI returned by
# ``/api/load-dataset``. If a future developer adds another dataset family,
# this map is the first place to update.
DATASET_CONFIGS: dict[str, DatasetConfig] = {
    "amazon": DatasetConfig(
        key="amazon",
        label="Amazon",
        description="products",
        filters=[
            {"name": "category", "label": "Category", "control": "categorical", "column": "category", "icon": "grid"},
            {"name": "brand", "label": "Brand", "control": "categorical", "column": "brand", "icon": "tags"},
            {"name": "best_seller", "label": "Best Seller", "control": "categorical", "column": "best_seller", "icon": "badge"},
            {"name": "price_max", "label": "Max Price", "control": "numeric_max", "column": "price", "icon": "sliders"},
            {"name": "rating_min", "label": "Min Rating", "control": "numeric_min", "column": "rating", "icon": "star"},
        ],
        ranking_attributes=["rating", "price", "sales_last_month"],
        display_columns=["model", "brand", "category", "rating", "price", "sales_last_month", "best_seller"],
        preview_columns=["model", "brand", "rating", "price"],
        default_filters={"category": ["headphones"], "price_max": 100, "rating_min": 4},
        default_sort="rating",
        default_direction="desc",
        bias_column="brand",
        bias_positive_values=["JBL"],
        bias_negative_values=["Skullcandy"],
    ),
    "pricerunner": DatasetConfig(
        key="pricerunner",
        label="PriceRunner",
        description="offers",
        filters=[
            {
                "name": "product_category",
                "label": "Product Category",
                "control": "categorical",
                "column": "product_category",
                "icon": "grid",
            },
            {"name": "seller", "label": "Seller", "control": "categorical", "column": "seller", "icon": "store"},
            {"name": "product_model", "label": "Product Model", "control": "categorical", "column": "product_model", "icon": "tags"},
        ],
        ranking_attributes=["product_id", "cluster_id", "seller"],
        display_columns=["product_model", "offer_title", "seller", "product_category", "product_id", "cluster_id"],
        preview_columns=["product_model", "seller", "product_id"],
        default_filters={},
        default_sort="product_id",
        default_direction="asc",
        bias_column="seller",
    ),
    "flights": DatasetConfig(
        key="flights",
        label="Flights",
        description="flights",
        filters=[
            {"name": "airline", "label": "Airline", "control": "categorical", "column": "airline", "icon": "plane"},
            {"name": "source", "label": "Origin", "control": "categorical", "column": "source", "icon": "pin"},
            {"name": "destination", "label": "Destination", "control": "categorical", "column": "destination", "icon": "pin"},
            {"name": "price_max", "label": "Max Price", "control": "numeric_max", "column": "price", "icon": "sliders"},
        ],
        ranking_attributes=["price", "days_until_departure", "duration"],
        display_columns=["airline", "source", "destination", "travel_class", "stops", "days_until_departure", "duration", "price"],
        preview_columns=["airline", "source", "destination", "price", "days_until_departure"],
        default_filters={"price_max": 10000},
        default_sort="price",
        default_direction="asc",
        bias_column="airline",
    ),
}


# Bundled demo sources shown in the dropdown. The app still supports generic
# file loading logic, but the current UI intentionally exposes a curated catalog
# so the paper scenarios are easy to reproduce.
SAMPLE_DATASETS = [
    {
        "id": "amazon_headphones_demo",
        "label": "Amazon Demo",
        "datasetType": "amazon",
        "path": str(BASE_DIR / "sample_data" / "amazon_headphones.csv"),
        "tag": "",
    },
    {
        "id": "amazon_demo",
        "label": "Amazon Products",
        "datasetType": "amazon",
        "path": str(BASE_DIR / "sample_data" / "amazon_products_with_categories.csv"),
        "tag": "",
    },
    {
        "id": "pricerunner_demo",
        "label": "PriceRunner Aggregate",
        "datasetType": "pricerunner",
        "path": str(BASE_DIR / "sample_data" / "pricerunner_aggregate.csv"),
        "tag": "",
    },
    {
        "id": "flights_demo",
        "label": "Flights Bucketized",
        "datasetType": "flights",
        "path": str(BASE_DIR / "sample_data" / "flights_bucketized.csv"),
        "tag": "",
    },
]


# Column aliases let us accept real datasets that do not use the exact field
# names expected by the UI. This keeps the frontend simple and pushes schema
# normalization into one backend layer.
COLUMN_ALIASES = {
    "model": ["model", "product_name", "title", "name"],
    "brand": ["brand", "manufacturer", "brand_name", "brandname"],
    "category": ["category", "main_category", "subcategory", "category_name"],
    "rating": ["rating", "stars", "review_score"],
    "price": ["price", "list_price", "amount", "fare"],
    "best_seller": ["best_seller", "best seller", "bestseller", "is_bestseller", "isBestSeller", "isbestseller"],
    "sales_last_month": [
        "sales_last_month",
        "sales in the last month",
        "monthly_sales",
        "last_month_sales",
        "boughtInLastMonth",
        "boughtinlastmonth",
    ],
    "seller": ["seller", "merchant", "shop_name", "MerchantID", "merchantid"],
    "product_model": ["product_model", "model", "product_name", "name", "title", "ClusterLabel", "clusterlabel"],
    "offer_title": ["ProductTitle", "producttitle", "offer_title"],
    "product_category": ["product_category", "category", "product_type", "CategoryLabel", "categorylabel"],
    "product_id": ["ProductID", "productid", "product_id"],
    "cluster_id": ["ClusterID", "clusterid", "cluster_id"],
    "airline": ["airline", "carrier", "airline_name"],
    "days_until_departure": ["days_until_departure", "days_left", "days_until_flight", "advance_purchase_days", "days_left"],
    "source": ["source", "origin", "from", "departure_city", "source_city"],
    "destination": ["destination", "dest", "to", "arrival_city", "destination_city"],
    "travel_class": ["class", "travel_class"],
    "stops": ["stops"],
    "departure_time": ["departure_time"],
    "arrival_time": ["arrival_time"],
    "duration": ["duration"],
}


# Canonical fields we try to extract for each dataset family. During loading we
# map raw dataset columns into these names using ``COLUMN_ALIASES``.
DATASET_FIELDS = {
    "amazon": ["model", "brand", "category", "rating", "price", "best_seller", "sales_last_month"],
    "pricerunner": ["offer_title", "product_model", "seller", "product_category", "product_id", "cluster_id"],
    "flights": ["airline", "source", "destination", "travel_class", "stops", "days_until_departure", "duration", "price"],
}


def canonicalize(name: str) -> str:
    return (
        str(name)
        .strip()
        .lower()
        .replace("-", "_")
        .replace("/", "_")
        .replace(" ", "_")
    )


def value_to_bool(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def display_value(value: Any) -> str:
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    if isinstance(value, int):
        return str(value)
    return str(value)


class AntiManipulationEngine:
    """In-memory demo engine used by the Flask routes.

    This object deliberately stores only one active dataset and one active query
    session. That keeps the prototype compact and makes the request flow easier
    to understand for a follow-up implementation.
    """

    def __init__(self) -> None:
        self.dataset_path: Path | None = None
        self.dataset_type: str | None = None
        self.df: pd.DataFrame | None = None
        self.last_query: dict[str, Any] | None = None
        self.last_reformulation: dict[str, Any] | None = None
        self.last_algorithm_trace: list[dict[str, Any]] = []

    def get_catalog(self) -> list[dict[str, Any]]:
        return SAMPLE_DATASETS

    def load_dataset(self, file_path: str) -> dict[str, Any]:
        """Load one dataset, detect its family, normalize it, and reset state."""
        path = Path(file_path).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"Dataset not found: {path}")

        header = self._read_header(path)
        dataset_type = self._detect_dataset_type(header)
        df = self._read_dataframe(path, dataset_type, header)
        normalized = self._normalize_dataframe(df, dataset_type)

        self.dataset_path = path
        self.dataset_type = dataset_type
        self.df = normalized
        self.last_query = None
        self.last_reformulation = None
        self.last_algorithm_trace = []
        return self.get_metadata()

    def get_metadata(self) -> dict[str, Any]:
        """Return dataset-aware UI metadata for the frontend.

        The frontend does not hardcode filters or ranking attributes. Instead,
        it requests this metadata whenever a dataset is loaded and then renders
        the appropriate controls dynamically.
        """
        if self.df is None or self.dataset_type is None:
            return {"loaded": False, "catalog": self.get_catalog()}

        config = DATASET_CONFIGS[self.dataset_type]
        filter_meta: list[dict[str, Any]] = []
        categorical_columns = [
            filter_def["column"]
            for filter_def in config.filters
            if filter_def["control"] == "categorical" and filter_def["column"] in self.df.columns
        ]
        for filter_def in config.filters:
            column = filter_def["column"]
            item = dict(filter_def)
            if filter_def["control"] == "categorical" and column in self.df.columns:
                # The current UI keeps categorical filters compact by always
                # using the search + add pattern and only surfacing two quick
                # picks per facet.
                counts = self.df[column].fillna("Unknown").map(display_value).value_counts()
                option_values = counts.index.tolist()
                item["options"] = option_values[: min(400, len(option_values))]
                item["topOptions"] = option_values[:2]
                item["ui"] = "search"
            elif column in self.df.columns:
                series = pd.to_numeric(self.df[column], errors="coerce").fillna(0)
                item["bounds"] = {
                    "min": float(series.min()),
                    "max": float(series.max()),
                    # Numeric sliders should be able to hit real dataset values,
                    # including edge values like 19.99 or 4.3. Using a coarse
                    # fixed step such as 0.5 makes those hard or impossible to
                    # select, so we derive the step from the actual data.
                    "step": self._numeric_step(series),
                }
            filter_meta.append(item)

        categorical_projection: list[dict[str, Any]] = []
        if categorical_columns:
            projection_df = self.df[categorical_columns].fillna("Unknown").copy()
            for column in categorical_columns:
                projection_df[column] = projection_df[column].map(display_value)
            # The frontend uses this de-duplicated projection to narrow the
            # available values in one categorical facet based on the selections
            # already made in the others.
            categorical_projection = projection_df.drop_duplicates().to_dict(orient="records")

        return {
            "loaded": True,
            "catalog": self.get_catalog(),
            "datasetType": config.key,
            "datasetLabel": config.label,
            "description": config.description,
            "rowCount": int(len(self.df)),
            "filters": filter_meta,
            "categoricalProjection": categorical_projection,
            "rankingAttributes": config.ranking_attributes,
            "displayColumns": config.display_columns,
            "previewColumns": config.preview_columns,
            "defaults": {
                "filters": self._resolved_defaults(config),
                "rankingMode": "custom",
                "rankingConditions": [
                    {
                        "attribute": config.default_sort,
                        "direction": config.default_direction,
                    }
                ],
                "sortBy": config.default_sort,
                "sortDirection": config.default_direction,
                "secondarySortBy": None,
                "secondarySortDirection": "asc",
                "topK": TOP_K,
            },
            "datasetPath": str(self.dataset_path),
        }

    def run_query(self, payload: dict[str, Any], mode: str = "original") -> dict[str, Any]:
        """Execute the main ranking flow for either original or generated query.

        ``mode="original"``:
            run the user intent against the biased/featured source behavior.

        ``mode="reformulated"``:
            apply the most recent generated CASE-based reformulation.
        """
        if self.df is None or self.dataset_type is None:
            raise ValueError("Load a dataset first.")

        config = DATASET_CONFIGS[self.dataset_type]
        filters = payload.get("filters", {})
        ranking_mode = payload.get("rankingMode") or "custom"
        ranking_conditions = self._normalize_ranking_conditions(payload, config)
        sort_by, sort_direction = ranking_conditions[0]
        secondary_sort_by = ranking_conditions[1][0] if len(ranking_conditions) > 1 else None
        secondary_sort_direction = ranking_conditions[1][1] if len(ranking_conditions) > 1 else "asc"
        top_k = int(payload.get("topK") or TOP_K)
        if ranking_mode not in {"custom", "featured"}:
            raise ValueError(f"Unsupported ranking mode: {ranking_mode}")

        filtered = self._apply_filters(self.df, filters, config)
        # ``intent_df`` is the "what the user wants" ordering. We keep it around
        # even when the visible result uses a biased ordering so later analysis
        # steps can compare the two.
        intent_df = self._rank_by_intent(filtered, ranking_conditions)

        if mode == "original":
            if ranking_mode == "featured":
                ranked = self._rank_featured(filtered)
            else:
                ranked = self._rank_by_biased_source(
                    intent_df,
                    ranking_conditions,
                )
        else:
            if not self.last_reformulation:
                raise ValueError("Run analysis before applying a reformulation.")
            if mode == "improved":
                improved = sample_improve_query(self.last_reformulation, sort_by)
                self.last_reformulation = improved["reformulation"]
                self.last_algorithm_trace.append(improved)
            ranked = self._rank_with_case_query(
                intent_df,
                ranking_conditions,
                self.last_reformulation,
            )

        visible = ranked.head(top_k).copy()
        visible["source_rank"] = range(1, len(visible) + 1)

        if mode == "original":
            # Only the original query becomes the new session baseline. The
            # generated query APIs build on top of this stored query.
            self.last_query = {
                "filters": filters,
                "rankingMode": ranking_mode,
                "rankingConditions": [
                    {"attribute": attribute, "direction": direction}
                    for attribute, direction in ranking_conditions
                ],
                "sortBy": sort_by,
                "sortDirection": sort_direction,
                "secondarySortBy": secondary_sort_by,
                "secondarySortDirection": secondary_sort_direction,
                "topK": top_k,
            }
            self.last_algorithm_trace = []

        return {
            "datasetType": self.dataset_type,
            "datasetLabel": config.label,
            "mode": mode,
            "rankingMode": ranking_mode,
            "rankingConditions": [
                {"attribute": attribute, "direction": direction}
                for attribute, direction in ranking_conditions
            ],
            "sortBy": sort_by,
            "sortDirection": sort_direction,
            "secondarySortBy": secondary_sort_by,
            "secondarySortDirection": secondary_sort_direction,
            "topK": top_k,
            "summary": self._summary_text(
                ranking_mode,
                mode,
                ranking_conditions,
                len(filtered),
                config,
            ),
            "resultColumns": ["tupleID", *config.display_columns],
            "result": self._records_for_output(visible, config.display_columns),
            "reformulation": self.last_reformulation,
            "algorithmTrace": self.last_algorithm_trace,
        }

    def analyze_trustworthiness(self) -> dict[str, Any]:
        """Flag suspicious visible tuples in the current top-k window.

        The frontend uses the returned row ids for highlighting and the
        returned reason text for hover explanations on those same rows.
        """
        if self.df is None or self.dataset_type is None or not self.last_query:
            raise ValueError("Run a query first.")

        config = DATASET_CONFIGS[self.dataset_type]
        filtered = self._apply_filters(self.df, self.last_query["filters"], config)
        ranking_mode = self.last_query.get("rankingMode", "custom")
        ranking_conditions = self._normalize_ranking_conditions(self.last_query, config)
        sort_by = ranking_conditions[0][0]
        top_k = int(self.last_query["topK"])

        intent_df = self._rank_by_intent(filtered, ranking_conditions)
        if ranking_mode == "featured":
            biased_df = self._rank_featured(filtered).head(top_k).copy()
        else:
            biased_df = self._rank_by_biased_source(
                intent_df,
                ranking_conditions,
            ).head(top_k).copy()

        detection = sample_detect_untrustworthy_tuples(
            intent_df=intent_df,
            biased_df=biased_df,
            sort_by=sort_by,
            bias_column=config.bias_column,
            display_columns=config.display_columns,
            clean_record=self._clean_record,
        )
        self.last_algorithm_trace = [detection]

        return {
            "flagged": detection["flagged"],
            "relativeConstraint": None,
            "notes": [],
            "reformulation": self.last_reformulation,
            "algorithmTrace": self.last_algorithm_trace,
        }

    def find_influential_query(self) -> dict[str, Any]:
        """Generate a first CASE-style reformulation from the current session."""
        if self.df is None or self.dataset_type is None or not self.last_query:
            raise ValueError("Run a query first.")

        config = DATASET_CONFIGS[self.dataset_type]
        filtered = self._apply_filters(self.df, self.last_query["filters"], config)
        ranking_conditions = self._normalize_ranking_conditions(self.last_query, config)
        sort_by, sort_direction = ranking_conditions[0]
        top_k = int(self.last_query["topK"])

        intent_df = self._rank_by_intent(filtered, ranking_conditions)
        biased_df = (
            self._rank_featured(filtered).head(top_k).copy()
            if self.last_query.get("rankingMode") == "featured"
            else self._rank_by_biased_source(
                intent_df,
                ranking_conditions,
            ).head(top_k).copy()
        )

        equilibrium = sample_check_influential_equilibrium(self.dataset_type, sort_by, config.bias_column)
        influential = sample_find_influential_query(
            intent_df,
            biased_df,
            sort_by,
            sort_direction,
            config,
            filters=self.last_query["filters"],
            top_k=top_k,
            extra_ordering=ranking_conditions[1:],
        )
        self.last_reformulation = influential["reformulation"]
        self.last_algorithm_trace = [equilibrium, influential]
        return {
            "relativeConstraint": influential["reformulation"]["relativeConstraint"],
            "reformulation": influential["reformulation"],
            "algorithmTrace": self.last_algorithm_trace,
        }

    def improve_query(self) -> dict[str, Any]:
        """Refine the most recent reformulation without changing the base query."""
        if self.df is None or self.dataset_type is None or not self.last_query:
            raise ValueError("Run a query first.")
        if not self.last_reformulation:
            raise ValueError("Find influential query first.")
        config = DATASET_CONFIGS[self.dataset_type]
        sort_by = self._normalize_ranking_conditions(self.last_query, config)[0][0]
        improved = sample_improve_query(self.last_reformulation, sort_by)
        self.last_reformulation = improved["reformulation"]
        self.last_algorithm_trace = [improved]
        return {
            "relativeConstraint": improved["reformulation"]["relativeConstraint"],
            "reformulation": improved["reformulation"],
            "algorithmTrace": self.last_algorithm_trace,
        }

    def run_submitted_query(self) -> dict[str, Any]:
        """Execute the edited reformulated query text against the current dataset.

        Unlike the earlier demo-only flow, this path actually runs the query
        text from the editor against the normalized dataset. That makes edits to
        SELECT / WHERE / ORDER BY / LIMIT affect the returned result directly.
        """
        if self.df is None or self.dataset_type is None:
            raise ValueError("Load a dataset first.")
        if not self.last_query:
            raise ValueError("Run a query first.")
        if not self.last_reformulation:
            raise ValueError("Find influential query first.")

        config = DATASET_CONFIGS[self.dataset_type]
        query_text = str(self.last_reformulation.get("queryText") or "").strip()
        if not query_text:
            raise ValueError("No reformulated query to submit.")

        query_frame = self._execute_query_text(query_text, config)
        visible = query_frame.copy()
        if "limit" not in query_text.lower():
            visible = visible.head(int(self.last_query.get("topK") or TOP_K)).copy()

        result_columns, result_records = self._records_from_query_output(visible)
        return {
            "datasetType": self.dataset_type,
            "datasetLabel": config.label,
            "mode": "submitted",
            "rankingMode": self.last_query.get("rankingMode", "custom"),
            "rankingConditions": self.last_query.get("rankingConditions", []),
            "sortBy": self.last_query.get("sortBy"),
            "sortDirection": self.last_query.get("sortDirection"),
            "secondarySortBy": self.last_query.get("secondarySortBy"),
            "secondarySortDirection": self.last_query.get("secondarySortDirection"),
            "topK": self.last_query.get("topK", TOP_K),
            "summary": f"submitted query • {len(result_records)} rows",
            "resultColumns": result_columns,
            "result": result_records,
            "reformulation": self.last_reformulation,
            "algorithmTrace": self.last_algorithm_trace,
        }

    def _read_header(self, path: Path) -> list[str]:
        """Read only column headers so dataset family detection stays cheap."""
        suffix = path.suffix.lower()
        if suffix == ".csv":
            return pd.read_csv(path, nrows=0).columns.tolist()
        if suffix == ".json":
            return pd.read_json(path, nrows=0).columns.tolist()
        if suffix == ".jsonl":
            return pd.read_json(path, lines=True, nrows=0).columns.tolist()
        if suffix == ".parquet":
            return pd.read_parquet(path).columns.tolist()
        raise ValueError(f"Unsupported dataset format: {suffix}")

    def _resolve_usecols(self, header: list[str], dataset_type: str) -> list[str]:
        """Pick the minimal useful column subset for the detected family."""
        normalized_to_raw = {canonicalize(col): col for col in header}
        usecols: list[str] = []
        for canonical in DATASET_FIELDS[dataset_type]:
            for alias in COLUMN_ALIASES.get(canonical, [canonical]):
                key = canonicalize(alias)
                if key in normalized_to_raw:
                    usecols.append(normalized_to_raw[key])
                    break
        return list(dict.fromkeys(usecols))

    def _read_dataframe(self, path: Path, dataset_type: str, header: list[str]) -> pd.DataFrame:
        """Load the dataset using the smallest useful schema when possible."""
        suffix = path.suffix.lower()
        usecols = self._resolve_usecols(header, dataset_type)
        if suffix == ".csv":
            return pd.read_csv(path, usecols=usecols or None)
        if suffix == ".json":
            return pd.read_json(path)
        if suffix == ".jsonl":
            return pd.read_json(path, lines=True)
        if suffix == ".parquet":
            return pd.read_parquet(path)
        raise ValueError(f"Unsupported dataset format: {suffix}")

    def _detect_dataset_type(self, columns: Any) -> str:
        """Guess the dataset family from its schema.

        The current heuristic is intentionally simple because the dropdown uses
        curated sample datasets. A production implementation could make this
        stricter or require an explicit dataset type.
        """
        normalized = {canonicalize(col) for col in columns}
        if {"days_left", "days_until_departure", "days_until_flight"} & normalized:
            return "flights"
        if {"merchantid", "merchant_id", "seller"} & normalized and {"categorylabel", "product_category", "category"} & normalized:
            return "pricerunner"
        return "amazon"

    def _normalize_dataframe(self, df: pd.DataFrame, dataset_type: str) -> pd.DataFrame:
        """Map raw columns into canonical names and fill required defaults."""
        renamed = {}
        normalized_columns = {canonicalize(col): col for col in df.columns}
        claimed_source_columns: set[str] = set()
        for canonical in DATASET_FIELDS[dataset_type]:
            aliases = COLUMN_ALIASES.get(canonical, [canonical])
            for alias in aliases:
                alias_key = canonicalize(alias)
                if alias_key not in normalized_columns:
                    continue
                source_column = normalized_columns[alias_key]
                if source_column in claimed_source_columns:
                    continue
                renamed[source_column] = canonical
                claimed_source_columns.add(source_column)
                break

        # After this point the rest of the app can assume one stable schema per
        # dataset family regardless of how the original file was named.
        cleaned = df.rename(columns=renamed).copy()
        cleaned.columns = [canonicalize(col) for col in cleaned.columns]

        if dataset_type == "amazon":
            for col in ["model", "brand", "category"]:
                cleaned[col] = cleaned.get(col, "Unknown")
            if "best_seller" not in cleaned.columns:
                cleaned["best_seller"] = False
            cleaned["best_seller"] = cleaned["best_seller"].apply(value_to_bool)
            if "sales_last_month" not in cleaned.columns:
                cleaned["sales_last_month"] = 0
        elif dataset_type == "pricerunner":
            for col in ["offer_title", "product_model", "seller", "product_category"]:
                cleaned[col] = cleaned.get(col, "Unknown")
            if "product_id" not in cleaned.columns:
                cleaned["product_id"] = 0
            if "cluster_id" not in cleaned.columns:
                cleaned["cluster_id"] = 0
        else:
            for col in ["airline", "source", "destination", "travel_class", "stops"]:
                cleaned[col] = cleaned.get(col, "Unknown")
            if "days_until_departure" not in cleaned.columns:
                cleaned["days_until_departure"] = 0
            if "duration" not in cleaned.columns:
                cleaned["duration"] = 0

        for numeric in ["rating", "price", "sales_last_month", "days_until_departure", "duration", "seller", "product_id", "cluster_id"]:
            if numeric in cleaned.columns:
                cleaned[numeric] = pd.to_numeric(cleaned[numeric], errors="coerce").fillna(0)

        cleaned = cleaned.reset_index(drop=True)
        # ``__row_id`` becomes the stable tuple identifier used by the UI.
        cleaned["__row_id"] = cleaned.index + 1
        return cleaned

    def _apply_filters(self, df: pd.DataFrame, filters: dict[str, Any], config: DatasetConfig) -> pd.DataFrame:
        """Apply the user-selected filters from the dynamic query builder."""
        current = df.copy()
        for filter_def in config.filters:
            name = filter_def["name"]
            control = filter_def["control"]
            column = filter_def["column"]
            raw = filters.get(name)
            if raw in (None, "", [], {}):
                continue

            if control == "categorical":
                values = raw if isinstance(raw, list) else [raw]
                normalized = {display_value(value) for value in values}
                current = current[current[column].map(display_value).isin(normalized)]
            elif control == "numeric_max":
                current = current[current[column] <= float(raw)]
            elif control == "numeric_min":
                current = current[current[column] >= float(raw)]

        return current.reset_index(drop=True)

    def _numeric_step(self, series: pd.Series) -> float:
        """Choose a slider step that matches the data precision.

        This keeps numeric filters inclusive at the edges by allowing the UI to
        select actual dataset values instead of rounding everything to broad
        half-step buckets.
        """
        numeric = pd.to_numeric(series, errors="coerce").dropna()
        if numeric.empty:
            return 1.0

        sample = numeric.head(1000)
        if sample.apply(lambda value: float(value).is_integer()).all():
            return 1.0

        decimal_places = 0
        for value in sample:
            text = f"{float(value):.6f}".rstrip("0").rstrip(".")
            if "." in text:
                decimal_places = max(decimal_places, len(text.split(".", 1)[1]))

        decimal_places = min(decimal_places, 4)
        if decimal_places == 0:
            return 1.0
        return 10 ** (-decimal_places)

    def _normalize_ranking_conditions(
        self,
        payload: dict[str, Any],
        config: DatasetConfig,
    ) -> list[tuple[str, str]]:
        """Return a validated ordered ranking-condition list.

        The UI can now send an arbitrary-length ``rankingConditions`` array.
        For backward compatibility we also still accept the older
        ``sortBy`` / ``secondarySortBy`` fields and normalize both formats into
        one shared representation.
        """
        raw_conditions = payload.get("rankingConditions")
        candidates: list[dict[str, Any]] = []
        if isinstance(raw_conditions, list):
            candidates = [item for item in raw_conditions if isinstance(item, dict)]
        else:
            candidates = [
                {
                    "attribute": payload.get("sortBy") or config.default_sort,
                    "direction": payload.get("sortDirection") or config.default_direction,
                }
            ]
            if payload.get("secondarySortBy"):
                candidates.append(
                    {
                        "attribute": payload.get("secondarySortBy"),
                        "direction": payload.get("secondarySortDirection") or "asc",
                    }
                )

        normalized: list[tuple[str, str]] = []
        seen: set[str] = set()
        for item in candidates:
            attribute = str(item.get("attribute") or "").strip()
            direction = str(item.get("direction") or config.default_direction).lower().strip()
            if attribute not in config.ranking_attributes or attribute in seen:
                continue
            if direction not in {"asc", "desc"}:
                raise ValueError(f"Unsupported sort direction: {direction}")
            normalized.append((attribute, direction))
            seen.add(attribute)

        if not normalized:
            normalized.append((config.default_sort, config.default_direction))
        return normalized

    def _rank_by_intent(
        self,
        df: pd.DataFrame,
        ranking_conditions: list[tuple[str, str]],
    ) -> pd.DataFrame:
        """Compute the clean user-intent ranking.

        This is the comparison baseline used by trust analysis and by the saved
        result summaries.
        """
        sort_columns = [attribute for attribute, _ in ranking_conditions]
        ascending = [direction == "asc" for _, direction in ranking_conditions]
        sort_columns.append("__row_id")
        ascending.append(True)
        ranked = df.sort_values(by=sort_columns, ascending=ascending, kind="mergesort").copy()
        ranked["intent_rank"] = range(1, len(ranked) + 1)
        return ranked

    def _bias_score(self, row: pd.Series) -> float:
        """Demo-only heuristic for source-side manipulation.

        This function is intentionally hand-crafted. It exists only so the
        frontend can demonstrate the paper workflow before the real backend
        implementation is plugged in.
        """
        if self.dataset_type == "amazon":
            score = 0.0
            brand = str(row.get("brand", "")).strip().lower()
            if brand in {"jbl", "apple", "sony", "bose"}:
                score += 1.2
            if brand in {"skullcandy"}:
                score -= 0.9
            if value_to_bool(row.get("best_seller", False)):
                score += 0.4
            if float(row.get("sales_last_month", 0) or 0) < 30:
                score += 0.8
            return score
        if self.dataset_type == "pricerunner":
            seller = float(row.get("seller", 0) or 0)
            return 1.4 if seller in {3, 6, 298, 31} else 0.0
        airline = str(row.get("airline", "")).strip().lower()
        score = 1.5 if airline in {"vistara", "air_india", "indigo"} else 0.0
        if float(row.get("days_until_departure", 0) or 0) <= 7:
            score += 0.6
        return score

    def _rank_by_biased_source(
        self,
        df: pd.DataFrame,
        ranking_conditions: list[tuple[str, str]],
    ) -> pd.DataFrame:
        """Simulate how a conflicted data source may reorder results."""
        ranked = df.copy()
        if "intent_rank" not in ranked.columns:
            ranked = self._rank_by_intent(ranked, ranking_conditions)
        sort_by, _sort_direction = ranking_conditions[0]
        ranked["bias_score"] = ranked.apply(self._bias_score, axis=1)
        max_bias = max(abs(float(ranked["bias_score"].min())), abs(float(ranked["bias_score"].max())), 1.0)
        ranked["normalized_bias"] = ranked["bias_score"] / max_bias

        # Start from the clean intent ranking, then allow source-side bias to
        # move tuples only a limited number of positions. This keeps the chosen
        # sort order recognizable while still making manipulation visible.
        if sort_by == "price":
            max_shift = 0.25
        else:
            max_shift = 0.85 if self.dataset_type == "amazon" else 0.65
        ranked["source_score"] = ranked["intent_rank"] - (ranked["normalized_bias"] * max_shift)

        sort_columns = ["source_score", "intent_rank"]
        ascending = [True, True]
        sort_columns.append("__row_id")
        ascending.append(True)
        ranked = ranked.sort_values(by=sort_columns, ascending=ascending, kind="mergesort").copy()
        ranked["source_rank"] = range(1, len(ranked) + 1)
        return ranked

    def _rank_featured(self, df: pd.DataFrame) -> pd.DataFrame:
        """Demo implementation of the UI's 'Featured' mode.

        The current behavior is random on purpose because the user explicitly
        asked to keep featured as a placeholder until the real backend exists.
        """
        ranked = df.sample(frac=1).copy()
        ranked["source_rank"] = range(1, len(ranked) + 1)
        ranked["intent_rank"] = ranked.get("intent_rank", pd.Series(range(1, len(ranked) + 1), index=ranked.index))
        ranked["bias_score"] = 0.0
        return ranked

    def _rank_with_case_query(
        self,
        df: pd.DataFrame,
        ranking_conditions: list[tuple[str, str]],
        reformulation: dict[str, Any],
    ) -> pd.DataFrame:
        """Apply the generated CASE-style reformulation to the intent ranking."""
        sort_by, sort_direction = ranking_conditions[0]
        ranked = df.copy()
        preferred = reformulation["preferredValue"]
        penalized = reformulation["penalizedValue"]
        bias_column = reformulation["biasColumn"]

        def case_weight(value: Any) -> int:
            if display_value(value) == str(preferred) or str(value) == str(preferred):
                return 0
            if display_value(value) == str(penalized) or str(value) == str(penalized):
                return 2
            return 1

        ranked["case_weight"] = ranked[bias_column].apply(case_weight)
        ranked["bias_score"] = ranked.apply(self._bias_score, axis=1)
        sort_columns = ["case_weight"]
        ascending = [True]
        for attribute, direction in ranking_conditions:
            sort_columns.append(attribute)
            ascending.append(direction == "asc")
        sort_columns.append("__row_id")
        ascending.append(True)
        ranked = ranked.sort_values(by=sort_columns, ascending=ascending, kind="mergesort").copy()
        ranked["source_rank"] = range(1, len(ranked) + 1)
        return ranked

    def _resolved_defaults(self, config: DatasetConfig) -> dict[str, Any]:
        """Adjust configured defaults to whatever values actually exist."""
        defaults = dict(config.default_filters)
        if self.df is None:
            return defaults
        for filter_def in config.filters:
            if filter_def["control"] != "categorical":
                continue
            column = filter_def["column"]
            default_value = defaults.get(filter_def["name"])
            if default_value is None or column not in self.df.columns:
                continue
            allowed = set(self.df[column].fillna("Unknown").map(display_value).unique().tolist())
            values = default_value if isinstance(default_value, list) else [default_value]
            valid = [value for value in values if str(value) in allowed]
            if valid:
                defaults[filter_def["name"]] = valid
            else:
                top_value = self.df[column].fillna("Unknown").map(display_value).value_counts().index.tolist()[:1]
                defaults[filter_def["name"]] = top_value
        return defaults

    def _records_for_output(self, df: pd.DataFrame, columns: list[str]) -> list[dict[str, Any]]:
        """Convert dataframe rows into UI-ready records with tuple ids."""
        records: list[dict[str, Any]] = []
        for _, row in df.iterrows():
            item = {"tupleID": f"e{int(row['__row_id'])}"}
            item.update(self._clean_record(row.to_dict(), columns))
            item["_rowId"] = int(row["__row_id"])
            records.append(item)
        return records

    def _clean_record(self, record: dict[str, Any], columns: list[str]) -> dict[str, Any]:
        """Limit exported fields and remove NaN/Inf values before JSON output."""
        cleaned: dict[str, Any] = {}
        for column in columns:
            value = record.get(column)
            if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                value = None
            cleaned[column] = value
        return cleaned

    def _clean_value(self, value: Any) -> Any:
        """Normalize one arbitrary SQL result cell for JSON output."""
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return None
        return value

    def _parse_query_row_id(self, value: Any) -> int:
        """Extract the stable tuple row id from ``tupleID`` or ``__row_id``."""
        text = str(value).strip()
        if text.lower().startswith("e"):
            text = text[1:]
        row_id = int(float(text))
        if row_id <= 0:
            raise ValueError
        return row_id

    def _execute_query_text(self, query_text: str, config: DatasetConfig) -> pd.DataFrame:
        """Run one edited SELECT query against the normalized in-memory dataset."""
        normalized_query = query_text.strip().rstrip(";")
        if not normalized_query.lower().startswith("select "):
            raise ValueError("Edited query must be a SELECT statement.")
        if ";" in normalized_query:
            raise ValueError("Only one SELECT statement is supported.")

        table_name = f"{config.key}_source"
        with sqlite3.connect(":memory:") as connection:
            dataset = self.df.copy()
            dataset.to_sql(table_name, connection, index=False, if_exists="replace")
            try:
                return pd.read_sql_query(normalized_query, connection)
            except Exception as error:
                raise ValueError(f"Edited query could not be executed: {error}") from error

    def _records_from_query_output(self, query_frame: pd.DataFrame) -> tuple[list[str], list[dict[str, Any]]]:
        """Convert SQL query output back into the frontend table contract."""
        result = query_frame.copy()
        if result.empty and "tupleID" not in result.columns and "__row_id" not in result.columns:
            raise ValueError("Edited query must select tupleID or __row_id.")

        if "tupleID" in result.columns:
            try:
                row_ids = result["tupleID"].map(self._parse_query_row_id)
            except Exception as error:
                raise ValueError("Edited query returned invalid tupleID values.") from error
            result["tupleID"] = row_ids.map(lambda value: f"e{value}")
        elif "__row_id" in result.columns:
            try:
                row_ids = result["__row_id"].map(self._parse_query_row_id)
            except Exception as error:
                raise ValueError("Edited query returned invalid __row_id values.") from error
            result.insert(0, "tupleID", row_ids.map(lambda value: f"e{value}"))
            result = result.drop(columns=["__row_id"])
        else:
            raise ValueError("Edited query must select tupleID or __row_id.")

        ordered_columns = ["tupleID", *[column for column in result.columns if column != "tupleID"]]
        records: list[dict[str, Any]] = []
        for row_id, (_, row) in zip(row_ids.tolist(), result.iterrows(), strict=False):
            item = {"_rowId": int(row_id)}
            for column in ordered_columns:
                item[column] = self._clean_value(row[column])
            records.append(item)
        return ordered_columns, records

    def _summary_text(
        self,
        ranking_mode: str,
        mode: str,
        ranking_conditions: list[tuple[str, str]],
        filtered_rows: int,
        config: DatasetConfig,
    ) -> str:
        """Return the terse summary text shown above the result table."""
        if ranking_mode == "featured":
            if mode == "original":
                return f"{filtered_rows} rows"
            if mode == "reformulated":
                return "influential result"
            return "improved result"
        if mode == "original":
            return f"{filtered_rows} rows"
        if mode == "reformulated":
            return "influential result"
        return "improved result"


app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False
engine = AntiManipulationEngine()


@app.errorhandler(Exception)
def handle_error(error: Exception):
    """Return JSON errors for both frontend fetches and API debugging."""
    return jsonify({"error": str(error)}), getattr(error, "code", 500)


@app.get("/")
def index() -> str:
    return render_template("index.html")


@app.get("/api/catalog")
def catalog():
    """Return the curated dataset dropdown contents."""
    return jsonify({"catalog": engine.get_catalog(), "metadata": engine.get_metadata()})


@app.post("/api/load-dataset")
def load_dataset():
    """Load one selected sample dataset and return dataset-aware UI metadata."""
    payload = request.get_json(force=True)
    return jsonify(engine.load_dataset(payload["path"]))


@app.post("/api/query")
def run_query():
    """Run the original query specified by the query builder."""
    return jsonify(engine.run_query(request.get_json(force=True), mode="original"))


@app.post("/api/analyze")
def analyze():
    """Highlight suspicious tuples in the current visible result."""
    return jsonify(engine.analyze_trustworthiness())


@app.post("/api/find-influential")
def find_influential():
    """Generate the first influential reformulation for the current query."""
    if not engine.last_query:
        return jsonify({"error": "Run a query first."}), 400
    return jsonify(engine.find_influential_query())


@app.post("/api/improve-query")
def improve_query():
    """Generate a refined version of the last reformulation."""
    if not engine.last_query:
        return jsonify({"error": "Run a query first."}), 400
    return jsonify(engine.improve_query())


@app.post("/api/submit-query")
def submit_query():
    """Apply the generated reformulation and return the updated ranking."""
    if not engine.last_query:
        return jsonify({"error": "Run a query first."}), 400
    if not engine.last_reformulation:
        return jsonify({"error": "Find influential query first."}), 400
    payload = request.get_json(silent=True) or {}
    edited_query_text = payload.get("queryText")
    if edited_query_text is not None:
        # The frontend can edit the SQL-like query directly before submission.
        # We persist the edited text and then execute it against the current
        # normalized dataset in ``run_submitted_query``.
        normalized_text = str(edited_query_text).strip()
        if normalized_text:
            engine.last_reformulation["queryText"] = normalized_text
    return jsonify(engine.run_submitted_query())


if __name__ == "__main__":
    app.run(debug=True)
