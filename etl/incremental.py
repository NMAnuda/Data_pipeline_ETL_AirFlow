import pandas as pd
from load import get_engine  # Reuse

def get_last_load_date(engine):
    """Get last successful load date from tracker table."""
    with engine.connect() as conn:
        result = conn.execute("SELECT last_load_date FROM load_tracker ORDER BY last_load_date DESC LIMIT 1;")
        date = result.fetchone()
        return pd.to_datetime(date[0]) if date else pd.to_datetime('2024-01-01')  # Default start

def update_last_load_date(engine, new_date):
    """Update tracker with new load date."""
    with engine.connect() as conn:
        conn.execute("INSERT INTO load_tracker (last_load_date) VALUES (%s) ON CONFLICT DO NOTHING;", (new_date,))
        conn.commit()

def apply_incremental_filter(df, last_date, date_col='Order Date'):
    """Filter DF to rows after last_date."""
    df[date_col] = pd.to_datetime(df[date_col], dayfirst=True, errors='coerce')  # DD-MM-YYYY fix
    return df[df[date_col] > last_date].copy()