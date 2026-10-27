import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import unittest
from unittest.mock import MagicMock, patch

def test_module_structure():
    import tab_data_validation as tdv
    
    # Check that key functions exist
    expected_funcs = [
        'render_validation_tab',
        'run_schema_check',
        'run_metrics_check',
        '_render_connection_guard',
        '_render_table_selectors',
        '_render_schema_result',
        '_render_metrics_result',
        '_rs_schemas',
        '_rs_tables',
        '_rs_row_count',
        '_rs_schema_info',
        '_rs_numeric_stats',
        '_rs_checksum',
        '_sf_schemas',
        '_sf_tables',
        '_sf_row_count',
        '_sf_schema_info',
        '_sf_numeric_stats',
        '_sf_hash_agg',
        '_normalise_type',
    ]
    for fn in expected_funcs:
        assert hasattr(tdv, fn), f"Missing function {fn}"
    print("All expected functions present.")

    # Check type normalisation
    assert tdv._normalise_type('VARCHAR(255)') == 'varchar'
    assert tdv._normalise_type('INT4') == 'integer'
    assert tdv._normalise_type('CHARACTER VARYING(100)') == 'varchar'
    assert tdv._normalise_type('TIMESTAMP WITHOUT TIME ZONE') == 'timestamp'
    assert tdv._normalise_type('NUMERIC(18, 2)') == 'numeric'
    print("Type normalisation verified.")

def test_schema_check_logic():
    import tab_data_validation as tdv
    import pandas as pd

    rs_df = pd.DataFrame([
        {'column_name': 'ID', 'data_type': 'integer', 'char_max_len': None, 'num_precision': 32, 'num_scale': 0},
        {'column_name': 'NAME', 'data_type': 'varchar', 'char_max_len': 100, 'num_precision': None, 'num_scale': None},
        {'column_name': 'AMOUNT', 'data_type': 'numeric', 'char_max_len': None, 'num_precision': 12, 'num_scale': 2},
    ])
    sf_df = pd.DataFrame([
        {'column_name': 'ID', 'data_type': 'NUMBER', 'char_max_len': None, 'num_precision': 38, 'num_scale': 0},
        {'column_name': 'NAME', 'data_type': 'TEXT', 'char_max_len': 16777216, 'num_precision': None, 'num_scale': None},
        {'column_name': 'AMOUNT', 'data_type': 'DECIMAL', 'char_max_len': None, 'num_precision': 12, 'num_scale': 2},
    ])

    with patch('tab_data_validation._rs_schema_info', return_value=rs_df), \
         patch('tab_data_validation._sf_schema_info', return_value=sf_df):
        res = tdv.run_schema_check({}, {}, 'public', 'orders', 'PUBLIC', 'ORDERS')
        assert res['common_cols'] == ['amount', 'id', 'name']
        assert res['only_in_rs'] == []
        assert res['only_in_src'] == []
        print("Schema check result:", res['passed'], "type_mismatches:", res['type_mismatches'])

def test_render_tab():
    import tab_data_validation as tdv
    import streamlit as st

    # Initialize mock session state
    st.session_state['rs_connected'] = False
    st.session_state['sf_connected'] = False
    
    # Should render connection guard without error
    tdv.render_validation_tab({}, {})
    print("Rendered with disconnected state successfully.")

    # Connected state
    st.session_state['rs_connected'] = True
    st.session_state['sf_connected'] = True
    st.session_state['rs_schemas'] = ['public', 'analytics']
    st.session_state['sf_schemas'] = ['PUBLIC', 'ANALYTICS']
    st.session_state['val2_rs_tables_map'] = {'public': ['orders', 'users']}
    st.session_state['val2_sf_tables_map'] = {'PUBLIC': ['ORDERS', 'USERS']}

    tdv.render_validation_tab({}, {})
    print("Rendered with connected state successfully.")

if __name__ == '__main__':
    test_module_structure()
    test_schema_check_logic()
    test_render_tab()
    print("ALL TESTS PASSED SUCCESSFULLY!")
