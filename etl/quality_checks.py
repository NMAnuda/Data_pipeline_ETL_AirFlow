from sqlalchemy import text
from load import get_engine  # Reuse your DB engine

def run_quality_checks(engine):
    """Run checks — log issues, return True if critical issues pass."""
    
    checks = [
        # Check nulls in key columns
        "SELECT COUNT(*) FROM fact_orders WHERE amount IS NULL OR profit IS NULL OR quantity IS NULL;",
        # Check for duplicates (unique order_id count vs total rows)
        "SELECT COUNT(DISTINCT order_id) AS unique_orders FROM fact_orders;",
        # Case-sensitive null checks
        "SELECT COUNT(*) FROM dim_customers WHERE \"CustomerName\" IS NULL;",
        "SELECT COUNT(*) FROM dim_products WHERE \"Category\" IS NULL OR \"Sub-Category\" IS NULL;",
        # Check foreign key integrity
        "SELECT COUNT(*) FROM fact_orders o LEFT JOIN dim_products p ON o.product_id = p.product_id WHERE p.product_id IS NULL;"
    ]
    
    results = []
    total_orders = None
    
    with engine.connect() as conn:
        # Get total rows for duplicate calculation
        total_result = conn.execute(text("SELECT COUNT(*) FROM fact_orders;"))
        total_orders = total_result.fetchone()[0] or 0
        
        for i, query in enumerate(checks):
            result = conn.execute(text(query)).fetchone()[0]
            results.append(result)
            
            if i == 1:  # Unique orders / duplicate check
                duplicates = total_orders - result
                duplicate_pct = (duplicates / total_orders * 100) if total_orders > 0 else 0
                print(f"Quality Check {i+1}: unique_orders = {result}, duplicates = {duplicates} ({duplicate_pct:.1f}% in fact_orders)")
            else:
                table_name = query.split('FROM')[1].split()[0]
                print(f"Quality Check {i+1}: {result} issues ({table_name})")
    
    # ------------------------------
    # Evaluate results (critical vs non-critical)
    # ------------------------------
    
    nulls_in_facts = results[0] > 0            # Critical: fail if any key nulls
    duplicate_pct = (total_orders - results[1]) / total_orders if total_orders > 0 else 0
    duplicates_in_facts = False                # Non-critical: only log duplicates
    if duplicate_pct > 0.1:
        print(f"WARNING: {duplicate_pct*100:.1f}% duplicates in fact_orders (allowed <10%)")
    
    nulls_in_customers = results[2] > 50      # Non-critical: allow up to 50 nulls
    if nulls_in_customers > 50:
        print(f"WARNING: {nulls_in_customers} null CustomerName values (allowed up to 50)")
    
    nulls_in_products = results[3] > 0        # Critical: fail if any null
    fk_violations = results[4] > 0            # Critical: fail if any FK violations
    
    # ------------------------------
    # Determine DAG outcome
    # ------------------------------
    if nulls_in_facts or nulls_in_products or fk_violations:
        print("CRITICAL QUALITY FAIL: Check logs! DAG may halt!")
        return False
    
    print("Quality checks completed. Non-critical issues may exist but DAG will continue. ✅")
    return True

# ------------------------------
# Usage example
# ------------------------------
if __name__ == "__main__":
    engine = get_engine()
    run_quality_checks(engine)
