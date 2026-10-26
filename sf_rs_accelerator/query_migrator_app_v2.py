"""
Snowflake to Redshift BI Query & Procedure Migration Tool - Streamlit App
Converts Snowflake-specific SQL and stored procedures to Redshift-compatible versions
"""

import streamlit as st
import sqlglot
import re
from typing import Optional, Tuple, List

# Import the stored procedure migrator from the main script
import sys
import os
sys.path.append(os.path.dirname(__file__))

try:
    from st_to_rs_bi_query_migration import StoredProcedureMigrator, BIQueryMigrator
except ImportError:
    StoredProcedureMigrator = None
    BIQueryMigrator = None

# Page config
st.set_page_config(
    page_title="Snowflake to Redshift Query & Procedure Migrator",
    page_icon="🔄",
    layout="wide"
)

# ============================================================
# STREAMLIT APP
# ============================================================

def main():
    from ui_theme import apply_theme, render_theme_toggle
    apply_theme()
    render_theme_toggle()

    # Header
    st.title("🔄 Snowflake to Redshift Migrator")
    st.markdown("Convert your Snowflake BI queries and stored procedures to Redshift-compatible SQL")
    st.markdown("---")
    
    # Create tabs for Query and Procedure migration
    tab_query, tab_procedure = st.tabs(["📊 Query Migration", "🔧 Stored Procedure Migration"])
    
    # ============================================================
    # QUERY MIGRATION TAB
    # ============================================================
    with tab_query:
        st.subheader("Query Migration")
        st.markdown("Convert Snowflake SELECT queries to Redshift-compatible syntax")
        
        # Create two columns for input and output
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📥 Snowflake Query")
            st.markdown("*Paste your Snowflake SQL here*")
            
            snowflake_query = st.text_area(
                "Snowflake SQL",
                height=400,
                placeholder="""SELECT 
    customer_id,
    NVL(region, 'Unknown') as region,
    COUNT(*) as order_count,
    SUM(amount) as total_amount
FROM orders
WHERE order_date >= DATEADD(month, -3, CURRENT_DATE())
GROUP BY customer_id, region""",
                label_visibility="collapsed",
                key="input_query"
            )
        
        with col2:
            st.subheader("📤 Converted Query")
            st.markdown("*Result will appear here*")
        
        # Run button
        st.markdown("---")
        col_btn1, col_btn2, col_btn3 = st.columns([1, 2, 1])
        
        with col_btn2:
            run_button_query = st.button("🚀 Convert Query", type="primary", use_container_width=True, key="btn_query")
        
        # Process when button is clicked
        if run_button_query and snowflake_query:
            if BIQueryMigrator is None:
                st.error("❌ BIQueryMigrator not available. Please ensure all dependencies are installed.")
            else:
                migrator = BIQueryMigrator()
                
                try:
                    converted, changes = migrator.migrate_query(snowflake_query)
                    
                    # Show success message
                    st.success("✅ Query converted successfully!")
                    
                    # Display changes
                    st.markdown("### 📋 Changes Made")
                    
                    if changes:
                        for change in changes:
                            if "⚠️" in change:
                                st.markdown(f'<div class="warning-item">⚠️ {change}</div>', unsafe_allow_html=True)
                            else:
                                st.markdown(f'<div class="change-item">✓ {change}</div>', unsafe_allow_html=True)
                    else:
                        st.info("No specific changes were required.")
                    
                    # Show the converted query in a code block
                    st.markdown("### 📤 Converted Redshift Query")
                    st.code(converted, language="sql")
                    
                except Exception as e:
                    st.error(f"Error converting query: {str(e)}")
                    st.markdown("""
                    ### 💡 Troubleshooting Tips:
                    - Check for unmatched parentheses or quotes
                    - Ensure all table and column names are valid
                    - Some complex Snowflake features may not be directly convertible
                    """)
    
    # ============================================================
    # STORED PROCEDURE MIGRATION TAB
    # ============================================================
    with tab_procedure:
        st.subheader("Stored Procedure Migration")
        st.markdown("Convert Snowflake stored procedures to Redshift-compatible format")
        
        if StoredProcedureMigrator is None:
            st.warning("⚠️ StoredProcedureMigrator not available. Please ensure st_to_rs_bi_query_migration.py is properly configured.")
        else:
            # Create two columns for input and output
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("📥 Snowflake Procedure")
                st.markdown("*Paste your Snowflake stored procedure here*")
                
                snowflake_procedure = st.text_area(
                    "Snowflake Procedure",
                    height=400,
                    placeholder="""CREATE OR REPLACE PROCEDURE sp_process_orders(p_order_date DATE)
RETURNS VARCHAR AS
$$
DECLARE
    v_order_count INTEGER;
    v_status VARCHAR(50);
BEGIN
    SELECT COUNT(*) INTO v_order_count
    FROM orders
    WHERE order_date = p_order_date;
    
    IF v_order_count > 100 THEN
        v_status := 'HIGH_VOLUME';
    ELSE
        v_status := 'LOW_VOLUME';
    END IF;
    
    RETURN v_status;
END;
$$
LANGUAGE SQL;""",
                    label_visibility="collapsed",
                    key="input_procedure"
                )
            
            with col2:
                st.subheader("📤 Converted Procedure")
                st.markdown("*Result will appear here*")
            
            # Run button
            st.markdown("---")
            col_btn1, col_btn2, col_btn3 = st.columns([1, 2, 1])
            
            with col_btn2:
                run_button_proc = st.button("🚀 Convert Procedure", type="primary", use_container_width=True, key="btn_procedure")
            
            # Process when button is clicked
            if run_button_proc and snowflake_procedure:
                proc_migrator = StoredProcedureMigrator()
                
                try:
                    # Check if it's a stored procedure
                    if proc_migrator.is_stored_procedure(snowflake_procedure):
                        converted_proc = proc_migrator.migrate_procedure(snowflake_procedure, preview=False)
                        
                        # Show success message
                        st.success("✅ Stored procedure converted successfully!")
                        
                        # Show important notes
                        st.markdown("### 📋 Migration Notes")
                        st.warning("""
                        **Important Considerations:**
                        - ⚠️ Snowflake native UDFs cannot be converted (skipped as requested)
                        - Test procedures thoroughly in your Redshift environment
                        - Some advanced features may require manual adjustment
                        - Review error handling and transaction control
                        - Ensure all referenced tables and columns exist in Redshift
                        """)
                        
                        # Show the converted procedure in a code block
                        st.markdown("### 📤 Converted Redshift Procedure")
                        st.code(converted_proc, language="sql")
                    else:
                        st.error("❌ Input does not appear to be a Snowflake stored procedure.")
                        st.info("Please ensure your input starts with 'CREATE PROCEDURE' or 'CREATE OR REPLACE PROCEDURE'")
                    
                except Exception as e:
                    st.error(f"Error converting procedure: {str(e)}")
                    st.markdown("""
                    ### 💡 Troubleshooting Tips:
                    - Verify the procedure syntax is correct
                    - Check for balanced parentheses and quotes
                    - Ensure the procedure definition is complete
                    - Some Snowflake-specific constructs may not be supported
                    """)
    
    # ============================================================
    # EXAMPLE QUERIES AND PROCEDURES
    # ============================================================
    st.markdown("---")
    st.markdown("### 📝 Example Queries and Procedures")
    
    with st.expander("🔍 Click to see example queries"):
        st.markdown("#### Example 1: Basic NVL conversion")
        st.code("""SELECT 
    customer_id,
    NVL(region, 'Unknown') as region,
    COUNT(*) as order_count
FROM orders
GROUP BY customer_id""", language="sql")
        
        st.markdown("#### Example 2: Window function")
        st.code("""SELECT 
    department,
    employee_name,
    salary,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rn
FROM employees""", language="sql")
        
        st.markdown("#### Example 3: String aggregation")
        st.code("""SELECT 
    department,
    STRING_AGG(employee_name, ',' ORDER BY salary DESC) as employees
FROM employees
GROUP BY department""", language="sql")
        
        st.markdown("#### Example 4: Date functions")
        st.code("""SELECT 
    order_date,
    DATE_TRUNC('month', order_date) as month_start,
    DATEDIFF(day, order_date, CURRENT_DATE()) as days_ago
FROM orders""", language="sql")
        
        st.markdown("#### Example 5: Conditional aggregation")
        st.code("""SELECT 
    department,
    COUNTIF(status = 'active') as active_count,
    COUNTIF(status = 'inactive') as inactive_count
FROM employees""", language="sql")
    
    with st.expander("🔍 Click to see example stored procedures"):
        st.markdown("#### Example 1: Simple procedure with variables")
        st.code("""CREATE OR REPLACE PROCEDURE sp_update_sales(p_region VARCHAR)
RETURNS TABLE AS
$$
DECLARE
    v_updated_rows INTEGER DEFAULT 0;
    v_current_date DATE := CURRENT_DATE();
BEGIN
    UPDATE sales_summary
    SET updated_date = v_current_date
    WHERE region = p_region;
    
    v_updated_rows := ROW_COUNT;
    
    SELECT v_updated_rows AS rows_updated;
END;
$$
LANGUAGE SQL;""", language="sql")
        
        st.markdown("#### Example 2: Procedure with IF-THEN-ELSE")
        st.code("""CREATE OR REPLACE PROCEDURE sp_process_orders(p_order_date DATE)
RETURNS VARCHAR AS
$$
DECLARE
    v_order_count INTEGER;
    v_status VARCHAR(50);
BEGIN
    SELECT COUNT(*) INTO v_order_count
    FROM orders
    WHERE order_date = p_order_date;
    
    IF v_order_count > 100 THEN
        v_status := 'HIGH_VOLUME';
    ELSIF v_order_count > 50 THEN
        v_status := 'MEDIUM_VOLUME';
    ELSE
        v_status := 'LOW_VOLUME';
    END IF;
    
    RETURN v_status;
END;
$$
LANGUAGE SQL;""", language="sql")
        
        st.markdown("#### Example 3: Procedure with loop")
        st.code("""CREATE OR REPLACE PROCEDURE sp_archive_records(p_days_old INTEGER)
RETURNS VARCHAR AS
$$
DECLARE
    v_archived_count INTEGER := 0;
    v_batch_size INTEGER := 1000;
BEGIN
    -- Archive old records in batches
    -- Loop and archive records
    RETURN 'Archived ' || v_archived_count || ' records';
END;
$$
LANGUAGE SQL;""", language="sql")


if __name__ == "__main__":
    main()
