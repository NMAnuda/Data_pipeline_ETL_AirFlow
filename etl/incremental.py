import pandas as pd
from sqlalchemy import text
from load import get_engine


def get_last_load_date(engine):
    """Get last successful load date from tracker table."""
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT MAX(last_load_date) FROM load_tracker;")
        ).fetchone()

    # SAFE DEFAULT — NEVER blocks historical loads
    if result is None or result[0] is None:
        return pd.Timestamp("1900-01-01")

    return pd.to_datetime(result[0])


def update_last_load_date(engine, new_date):
    """Update tracker with new load date."""
    with engine.begin() as conn:  # auto-commit
        conn.execute(
            text("INSERT INTO load_tracker (last_load_date) VALUES (:d)"),
            {"d": new_date}
        )


def apply_incremental_filter(df, last_date, date_col='Order Date'):
    """Filter DataFrame to rows after last_date."""
    df[date_col] = pd.to_datetime(
        df[date_col],
        dayfirst=True,
        errors='coerce'
    )

    filtered = df[df[date_col] > last_date].copy()

    if filtered.empty:
        print("⚠️ Incremental filter returned 0 rows")

    return filtered
