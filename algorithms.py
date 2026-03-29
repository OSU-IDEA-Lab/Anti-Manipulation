"""Sample algorithm hooks for the Anti-Manipulation prototype.

These functions intentionally do *not* implement the full research algorithms
from the paper. They provide stable input/output contracts so a later backend
developer can replace them with the real logic while preserving the Flask API
and frontend behavior.

Each function returns a small dictionary that is easy to serialize and easy for
the UI to interpret. The important handoff point is not the exact placeholder
values, but the contract:

- what inputs the UI/backend can already provide,
- what outputs the UI expects back,
- where a real implementation should plug in.
"""

from __future__ import annotations

from typing import Any

import pandas as pd


def _sql_literal(value: Any) -> str:
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _build_demo_query_text(
    config,
    filters: dict[str, Any] | None,
    top_k: int | None,
    order_by_parts: list[str],
) -> str:
    """Build a readable SQL-like reformulation for the UI editor."""
    where_parts: list[str] = []

    for filter_def in config.filters:
        filter_name = filter_def["name"]
        raw_value = (filters or {}).get(filter_name)
        if raw_value in (None, "", [], {}):
            continue

        column = filter_def["column"]
        control = filter_def["control"]
        if control == "categorical":
            values = raw_value if isinstance(raw_value, list) else [raw_value]
            where_parts.append(f"{column} IN ({', '.join(_sql_literal(value) for value in values)})")
        elif control == "numeric_min":
            where_parts.append(f"{column} >= {_sql_literal(raw_value)}")
        elif control == "numeric_max":
            where_parts.append(f"{column} <= {_sql_literal(raw_value)}")

    where_clause = "TRUE" if not where_parts else "\n  AND ".join(where_parts)
    limit_value = top_k if top_k is not None else 16

    return (
        f"SELECT *\n"
        f"FROM {config.key}_source\n"
        f"WHERE {where_clause}\n"
        f"ORDER BY {', '.join(order_by_parts)}\n"
        f"LIMIT {limit_value}"
    )


def sample_check_influential_equilibrium(
    dataset_type: str,
    sort_by: str,
    bias_column: str,
) -> dict[str, Any]:
    """Placeholder for the paper's equilibrium-checking step.

    A future implementation can replace this with the actual game-theoretic
    check and keep the response shape intact.
    """
    return {
        "algorithm": "check_influential_equilibrium",
        "status": "ready",
        "hasInfluentialEquilibrium": True,
        "output": f"{dataset_type}:{sort_by}->{bias_column}",
    }


def sample_detect_untrustworthy_tuples(
    intent_df: pd.DataFrame,
    biased_df: pd.DataFrame,
    sort_by: str,
    bias_column: str,
    display_columns: list[str],
    clean_record,
) -> dict[str, Any]:
    """Flag visible tuples that look suspicious under the demo heuristic.

    Current behavior:
    - compare shown rank to clean intent rank,
    - look for missing higher-intent alternatives,
    - use the demo ``bias_score`` as an extra signal,
    - cap the output to a few visible flags so the UI stays readable.

    Expected replacement:
    - compute trust/manipulation scores using the real model from the paper.
    """
    def demo_bias_value(raw_bias_score: float) -> float:
        """Collapse the demo bias score into a paper-style bias indicator."""
        if raw_bias_score >= 0.25:
            return 1.0
        if raw_bias_score <= -0.25:
            return -1.0
        return 0.0

    def demo_bias_interval(
        bias_value: float,
        rank_gap: int,
        raw_bias_score: float,
        missing_count: int,
    ) -> tuple[float, float, float, float]:
        """Return demo ``g(z)``, ``s(z)``, and the resulting bias interval.

        These values are placeholders, not the exact paper equations. They are
        shaped to look like the running example so the UI can explain *why* a
        tuple was flagged, while leaving a clear seam for the real backend.
        """
        g_z = 1.0 + (0.25 * rank_gap) + (0.10 * missing_count)
        s_z = 0.90 + (0.10 * rank_gap) + (0.10 * min(abs(raw_bias_score), 3.0))
        interval_lower = bias_value - g_z
        interval_upper = bias_value - max(g_z - 1.0, s_z)
        return g_z, s_z, interval_lower, interval_upper

    def intersects_bias_range(
        interval_lower: float,
        interval_upper: float,
        bias_min: float,
        bias_max: float,
    ) -> bool:
        return interval_lower < bias_max and interval_upper > bias_min

    biased_ids = set(biased_df["__row_id"].tolist())
    candidate_pool = intent_df.head(max(len(biased_df) * 3, 1))
    flagged_candidates = []
    bias_min, bias_max = -1.0, 1.0

    for _, row in biased_df.iterrows():
        intent_rank = int(row["intent_rank"])
        source_rank = int(row["source_rank"])
        higher_missing = [
            item
            for _, item in candidate_pool.iterrows()
            if int(item["intent_rank"]) < intent_rank and int(item["__row_id"]) not in biased_ids
        ]
        raw_bias_score = float(row.get("bias_score", 0) or 0)
        rank_gap = max(intent_rank - source_rank, 0)
        bias_value = demo_bias_value(raw_bias_score)
        g_z, s_z, interval_lower, interval_upper = demo_bias_interval(
            bias_value=bias_value,
            rank_gap=rank_gap,
            raw_bias_score=raw_bias_score,
            missing_count=len(higher_missing),
        )
        suspicious = (
            (rank_gap >= 2 or bool(higher_missing) or raw_bias_score > 0.8)
            and intersects_bias_range(interval_lower, interval_upper, bias_min, bias_max)
        )
        if not suspicious:
            continue

        row_id = int(row["__row_id"])
        severity = rank_gap + (2 if raw_bias_score > 0.8 else 0)
        bias_value_label = str(row.get(bias_column, "Unknown"))
        reason = (
            f"Untrustworthy tuple: {bias_column}={bias_value_label}. "
            f"b(e{row_id})={bias_value:.0f}; interval [{interval_lower:.2f}, {interval_upper:.2f}) "
            f"intersects [{bias_min:.0f}, {bias_max:.0f}]. "
        )
        flagged_candidates.append(
            {
                "severity": severity,
                "rowId": row_id,
                "shownRank": source_rank,
                "intentRank": intent_rank,
                "reason": reason,
                "record": clean_record(row.to_dict(), display_columns),
            }
        )

    flagged_candidates.sort(key=lambda item: (-item["severity"], item["shownRank"]))
    flagged = [
        {key: value for key, value in item.items() if key != "severity"}
        for item in flagged_candidates[:4]
    ]

    return {
        "algorithm": "detect_untrustworthy_tuples",
        "status": "ready",
        "flagged": flagged,
        "output": f"flagged:{len(flagged)}",
    }


def sample_find_influential_query(
    intent_df: pd.DataFrame,
    biased_df: pd.DataFrame,
    sort_by: str,
    sort_direction: str,
    config,
    filters: dict[str, Any] | None = None,
    top_k: int | None = None,
    extra_ordering: list[tuple[str, str]] | None = None,
) -> dict[str, Any]:
    """Generate a first CASE-based reformulation.

    The current implementation infers over-represented and under-represented
    values in the visible result window, then builds a simple ORDER BY CASE
    clause. This mirrors the paper's demo flow without claiming to be the final
    algorithm.
    """
    bias_column = config.bias_column
    intent_counts = intent_df.head(max(len(biased_df) * 3, 1))[bias_column].astype(str).value_counts()
    biased_counts = biased_df[bias_column].astype(str).value_counts()

    promoted: list[str] = []
    demoted: list[str] = []

    for value, count in biased_counts.items():
        if count > int(intent_counts.get(value, 0)):
            promoted.append(value)

    for value, count in intent_counts.items():
        if count > int(biased_counts.get(value, 0)):
            demoted.append(value)

    preferred_value = (demoted or config.bias_negative_values or ["Other"])[0]
    penalized_value = (promoted or config.bias_positive_values or ["Other"])[0]

    order_by_parts = [
        (
            f"CASE WHEN {bias_column} = '{preferred_value}' THEN 0 "
            f"WHEN {bias_column} = '{penalized_value}' THEN 2 ELSE 1 END"
        ),
        f"{sort_by} {sort_direction.upper()}",
        *[
            f"{attribute} {direction.upper()}"
            for attribute, direction in (extra_ordering or [])
        ],
    ]

    query_text = _build_demo_query_text(
        config=config,
        filters=filters,
        top_k=top_k,
        order_by_parts=order_by_parts,
    )

    reformulation = {
        "queryText": query_text,
        "biasColumn": bias_column,
        "preferredValue": preferred_value,
        "penalizedValue": penalized_value,
        "relativeConstraint": (
            f"{config.bias_column}:{preferred_value}>{penalized_value}; {sort_by}:{sort_direction}"
        ),
        "notes": [
            f"promoted:{penalized_value}",
            f"preferred:{preferred_value}",
        ],
    }

    return {
        "algorithm": "find_influential_query",
        "status": "ready",
        "reformulation": reformulation,
        "output": "query:ready",
    }


def sample_improve_query(reformulation: dict[str, Any], sort_by: str) -> dict[str, Any]:
    """Apply a second demo-only refinement step to the generated query.

    The current placeholder just appends a tie-break marker so the UI can
    distinguish "initial reformulation" from "improved reformulation".
    """
    improved = dict(reformulation)
    query_text = reformulation["queryText"]
    tie_break = f"__intent_{sort_by}_tie_break"
    if "\nLIMIT " in query_text:
        prefix, suffix = query_text.rsplit("\nLIMIT ", 1)
        improved["queryText"] = f"{prefix}, {tie_break}\nLIMIT {suffix}"
    else:
        improved["queryText"] = f"{query_text}, {tie_break}"
    improved["notes"] = list(reformulation.get("notes", [])) + [
        f"tie_break:{sort_by}"
    ]
    return {
        "algorithm": "improve_user_utility",
        "status": "ready",
        "reformulation": improved,
        "output": "query:improved",
    }
