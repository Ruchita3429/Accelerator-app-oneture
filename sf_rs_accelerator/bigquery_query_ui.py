"""BigQuery query migration UI, with state separate from the Snowflake panels."""
import difflib
import hashlib
import json

import streamlit as st

from bigquery_query_migrator import BigQueryQueryMigrator, validate_redshift_syntax


EXAMPLE = """SELECT
  customer_id,
  IFNULL(region, 'Unknown') AS region,
  COUNTIF(amount > 0) AS paid_orders,
  SUM(amount) AS total_amount
FROM `my-project.sales.orders`
WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY customer_id, region"""


def render_query_migrator():
    st.markdown('### BigQuery → Redshift Query Migrator')
    st.caption('Convert GoogleSQL SELECT queries locally. No BigQuery or Redshift connection is required.')
    st.info('Supports flat-table SELECT queries, joins, CTEs, aggregates and window functions. BigQuery scripts, stored procedures, nested data and unsupported functions are reported for manual migration.')
    source_mode = st.radio('SQL input', ['Paste SQL', 'Upload .sql file'], horizontal=True, key='bq_qm_source_mode')
    source = ''
    if source_mode == 'Paste SQL':
        source = st.text_area('BigQuery SQL', height=300, placeholder=EXAMPLE, key='bq_qm_input')
    else:
        upload = st.file_uploader('BigQuery SQL file', type=['sql'], key='bq_qm_upload')
        if upload is not None:
            if upload.size > 1024 * 1024:
                st.error('For this POC, upload a SQL file no larger than 1 MiB.')
                return
            try:
                source = upload.getvalue().decode('utf-8-sig')
            except UnicodeDecodeError:
                st.error('Save the SQL file as UTF-8 and upload it again.')
                return
            st.code(source, language='sql')
    left, right = st.columns(2)
    with left:
        target_schema = st.text_input('Target Redshift schema', value='public', key='bq_qm_schema')
    with right:
        st.caption('By default, project/dataset qualifiers become the selected target schema and table names become lowercase. CTE names are retained.')
    with st.expander('Optional table mappings'):
        mapping_text = st.text_area('Source-to-target table mappings (JSON)', value='{}',
                                    placeholder='{"my-project.sales.orders": "public.orders"}', key='bq_qm_mappings')
        st.caption('Use the exact source reference as written in SQL (without backticks), mapped to schema.table. Use distinct targets when source tables have the same name.')
    fingerprint = hashlib.sha256(json.dumps([source, target_schema, mapping_text]).encode()).hexdigest()
    if st.button('Convert BigQuery Query', key='bq_qm_convert', type='primary'):
        st.session_state.pop('bq_qm_result', None)
        st.session_state.pop('bq_qm_validation', None)
        try:
            mappings = json.loads(mapping_text or '{}')
            result = BigQueryQueryMigrator(target_schema, mappings).migrate(source)
            st.session_state.bq_qm_result = result.to_dict()
            st.session_state.bq_qm_fingerprint = fingerprint
        except (ValueError, TypeError) as exc:
            st.error(f'Conversion settings: {exc}')
    result = st.session_state.get('bq_qm_result')
    if not result:
        return
    if st.session_state.get('bq_qm_fingerprint') != fingerprint:
        st.info('The SQL or mapping settings changed. Convert again to refresh the output.')
        return
    for error in result['errors']:
        st.error(error)
    if result['errors']:
        st.caption('No converted file is produced until every statement can be converted. Split the file to migrate supported queries separately.')
        return
    st.markdown('#### Redshift SQL')
    st.code(result['sql'], language='sql')
    st.caption(f"{result['statement_count']} statement(s) converted. SQL has not been executed.")
    with st.expander('Changes applied', expanded=True):
        for change in result['changes']:
            st.write(change)
    for warning in result['warnings']:
        st.warning(warning)
    with st.expander('SQL differences'):
        diff = '\n'.join(difflib.unified_diff(source.splitlines(), result['sql'].splitlines(),
                                           fromfile='bigquery.sql', tofile='redshift.sql', lineterm=''))
        st.code(diff, language='diff')
    if st.button('Check Redshift Syntax', key='bq_qm_validate'):
        st.session_state.bq_qm_validation = validate_redshift_syntax(result['sql'])
    validation = st.session_state.get('bq_qm_validation')
    if validation:
        (st.success if validation['valid'] else st.error)(validation['message'])
    # Notes remain in the download so offline reviewers see the same caveats.
    notes = '\n'.join('-- Review: ' + warning.replace('\n', ' ') for warning in result['warnings'])
    st.download_button('Download Redshift SQL', data=notes + '\n\n' + result['sql'] + '\n',
                       file_name='bigquery_to_redshift.sql', mime='text/plain', key='bq_qm_download')
