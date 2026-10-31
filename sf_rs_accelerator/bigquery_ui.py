"""BigQuery panels plugged into the existing Streamlit application."""
import hashlib
from datetime import datetime

import streamlit as st

from pipeline_routes import BQ_S3, BQ_REDSHIFT, formats_for, reset_queue, resolve_route


def render_connection():
    from bigquery_connector import create_client, list_datasets

    with st.expander('Google Cloud — BigQuery', expanded=False):
        project = st.text_input('Project ID', key='bq_project_input').strip()
        method = st.selectbox('Authentication', ['Service-account JSON', 'Application Default Credentials'], key='bq_auth')
        upload = st.file_uploader('Service-account JSON key', type=['json'], key='bq_key') if method == 'Service-account JSON' else None
        payload = upload.getvalue() if upload else None
        identity = hashlib.sha256(project.encode() + method.encode() + (payload or b'')).hexdigest()
        if st.session_state.get('bq_connected') and identity != st.session_state.get('bq_connection_id'):
            _disconnect()
        if st.button('Connect BigQuery', key='bq_connect'):
            client = None
            try:
                if method == 'Service-account JSON' and not payload:
                    raise ValueError('Upload a service-account JSON key first.')
                client = create_client(project, payload)
                datasets = list_datasets(client)
                _disconnect()
                st.session_state.update(bq_client=client, bq_connected=True,
                    bq_connection_id=identity, bq_datasets=datasets, bq_tables={})
                st.success(f'Connected · {len(datasets)} datasets')
            except Exception as exc:
                if client is not None: client.close()
                _disconnect()
                st.error(f'BigQuery connection failed: {exc}')
        if st.session_state.get('bq_connected'):
            st.caption('● BigQuery connected')
            if st.button('Disconnect BigQuery', key='bq_disconnect'):
                _disconnect()
                st.rerun()
        else:
            st.caption('○ BigQuery not connected')


def _disconnect():
    client = st.session_state.pop('bq_client', None)
    if client is not None:
        client.close()
    st.session_state.update(bq_connected=False, bq_tables={}, bq_datasets=[])
    for key in ('bq_queue_tables', 'bq_export_tables', 'bq_discover_datasets',
                'bq_ddl_tables', 'bq_ddl_metadata_selected',
                'bq_ddl_metadata_result', 'bq_ddl_meta_validation'):
        st.session_state.pop(key, None)
    if any(j.get('source_type') == 'bigquery' for j in st.session_state.get('job_queue', [])):
        reset_queue(st.session_state)


def config_fingerprint(s3_cfg, rs_cfg, route):
    import json
    values = {'s3': s3_cfg, 'rs': rs_cfg if route == BQ_REDSHIFT else {}, 'route': route}
    return hashlib.sha256(json.dumps(values, sort_keys=True).encode()).hexdigest()


def build_queue(project, selected, table_map, settings, s3_cfg, rs_cfg, connection_id):
    import uuid
    route = resolve_route(settings['destination'], 'BigQuery')
    settings = {**settings, 'destination': route}
    fmt = settings['file_format']
    if fmt not in formats_for(route): raise ValueError('Choose a valid file format for this route.')
    if route == BQ_REDSHIFT and not settings['rs_schema']:
        raise ValueError('Enter the target Redshift schema.')
    if settings.get('source_query') and len(selected) != 1:
        raise ValueError('Custom SQL requires exactly one selected table.')
    ts = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    prefix = s3_cfg.get('prefix', '').strip('/')
    queue = []
    run_id = uuid.uuid4().hex
    for dataset, table in selected:
        info = next(t for t in table_map[dataset] if t['table'] == table)
        if fmt == 'Iceberg':
            key = '/'.join(p for p in (prefix, dataset.lower(), table.lower()) if p) + '/'
        else:
            filename = settings.get('filename', '').strip() or f'{table}_{ts}.{fmt.lower()}'
            if '/' in filename or '\\' in filename or not filename.lower().endswith('.' + fmt.lower()):
                raise ValueError(f'Filename must be a base filename ending in .{fmt.lower()}.')
            if len(selected) > 1 and settings.get('filename'):
                raise ValueError('Custom filename requires exactly one selected table.')
            key = '/'.join(p for p in (prefix, dataset, filename) if p)
        queue.append({**settings, 'id': f'bigquery:{project}.{dataset}.{table}',
            'run_id': run_id, 'source_type': 'bigquery', 'bq_project': project,
            'bq_dataset': dataset, 'bq_table': table, 'bq_location': info['location'],
            'bq_connection_id': connection_id,
            'config_fingerprint': config_fingerprint(s3_cfg, rs_cfg, route),
            # Compatibility fields keep the existing report/export layout usable.
            'sf_schema': dataset, 'sf_table': table,
            'rs_table': settings.get('rs_table') or table.lower(), 's3_key': key,
            'status': 'pending', 'phase': 'pending', 'rows': 0, 'bytes': 0,
            'duration_secs': 0.0, 'error': '', 'include_header': False, 'delimiter': ','})
    targets = [(j['rs_schema'], j['rs_table']) for j in queue]
    if route == BQ_REDSHIFT and len(targets) != len(set(targets)):
        raise ValueError('Selected tables map to the same Redshift target. Build separate queues with distinct targets.')
    return queue


def render_discovery(s3_cfg, rs_cfg):
    from bigquery_connector import discover_tables

    st.markdown('### BigQuery table discovery')
    if not st.session_state.get('bq_connected'):
        st.info('Connect BigQuery in the sidebar first.')
        return
    datasets = st.multiselect('Datasets to discover', st.session_state.get('bq_datasets', []), key='bq_discover_datasets')
    if st.button('Discover BigQuery tables', disabled=not datasets, key='bq_discover'):
        try:
            with st.spinner('Reading BigQuery table metadata…'):
                result = discover_tables(st.session_state.bq_client, datasets)
            st.session_state.bq_tables = result
            for key in ('bq_queue_tables', 'bq_export_tables'):
                st.session_state.pop(key, None)
        except Exception as exc:
            st.error(f'Discovery failed: {exc}')
    _selection_form('queue', s3_cfg, rs_cfg, execute=False)


def _selection_form(scope, s3_cfg, rs_cfg, execute):
    from chunk_02_job_queue_engine import run_migration

    table_map = st.session_state.get('bq_tables', {})
    options = {f'{d}.{t["table"]}': (d, t['table']) for d, ts in table_map.items() for t in ts}
    if not options:
        st.info('Discover tables in the Table Discovery tab.')
        return
    selected = st.multiselect('BigQuery tables', list(options), key=f'bq_{scope}_tables')
    if selected:
        info_rows = []
        for label in selected:
            dataset, name = options[label]
            info = next(t for t in table_map[dataset] if t['table'] == name)
            info_rows.append({'Table': label, 'Rows': info.get('rows'), 'Bytes': info.get('bytes'), 'Location': info['location']})
        st.dataframe(info_rows, hide_index=True)
        if len(selected) == 1:
            with st.expander('Source schema'):
                st.dataframe(info.get('schema', []), hide_index=True)
    route = resolve_route(st.session_state.pipeline_destination, 'BigQuery')
    formats = formats_for(route)
    format_key = f'bq_{scope}_format'
    if st.session_state.get(format_key) not in formats:
        st.session_state[format_key] = formats[0]
    fmt = st.selectbox('File Format', formats, key=format_key)
    if fmt == 'Iceberg':
        st.caption('Creates or replaces an Iceberg table in S3, registered in AWS Glue. Layout: prefix/dataset/table/data/ and metadata/.')
    else:
        st.caption('Layout: prefix/dataset/table_timestamp.' + fmt.lower())
    compression = st.checkbox('GZIP compression', key=f'bq_{scope}_gzip', disabled=fmt not in ('Parquet', 'JSON'))
    row_limit = st.number_input('Row limit per table (0 = all within POC cap)', 0, 100000, 0, key=f'bq_{scope}_limit')
    st.caption('POC limits: 100,000 result rows, 1 GiB billed query bytes, 256 MiB Arrow result. Use small demo tables.')
    filter_expression = st.text_input('Filter expression (optional, without WHERE)', key=f'bq_{scope}_filter')
    query = st.text_area('Custom SELECT (optional, single table; overrides filter)', key=f'bq_{scope}_query', disabled=len(selected) != 1)
    filename = st.text_input('Output filename (optional)', key=f'bq_{scope}_filename', disabled=len(selected) != 1 or fmt == 'Iceberg')
    schema, target_table, mode = '', '', 'TRUNCATE & LOAD'
    if route == BQ_REDSHIFT:
        schema = st.text_input('Target Redshift schema', value='public', key=f'bq_{scope}_rs_schema')
        target_table = st.text_input('Target table override (single table only)', key=f'bq_{scope}_target', disabled=len(selected) != 1)
        mode = st.selectbox('Load mode', ['TRUNCATE & LOAD', 'APPEND'], key=f'bq_{scope}_load')
        st.caption('Target tables must already exist with matching columns and order. Default target table names are lowercase.')
    label = ('Export to S3' if route == BQ_S3 else 'Migrate to Redshift') if execute else 'Build Migration Queue'
    if st.button(label, key=f'bq_{scope}_build', disabled=not selected):
        try:
            if not st.session_state.get('bq_connected'):
                raise ValueError('Connect BigQuery first.')
            if execute and route == BQ_REDSHIFT and not st.session_state.get('rs_connected'):
                raise ValueError('Connect Redshift first.')
            settings = dict(destination=route, file_format=fmt,
                compression=compression and fmt in ('Parquet', 'JSON'), row_limit=row_limit,
                filter_expression=filter_expression, source_query=query if len(selected) == 1 else '',
                filename=filename if len(selected) == 1 and fmt != 'Iceberg' else '',
                rs_schema=schema.strip(), rs_table=target_table.strip() if len(selected) == 1 else '', load_mode=mode)
            queue = build_queue(st.session_state.bq_client.project, [options[n] for n in selected],
                table_map, settings, s3_cfg, rs_cfg, st.session_state.bq_connection_id)
            reset_queue(st.session_state)
            st.session_state.job_queue = queue
            if execute:
                run_migration({'client': st.session_state.bq_client}, s3_cfg, rs_cfg,
                              st.progress(0), st.empty())
            else:
                st.success(f'{len(queue)} tables queued. Run them from the BigQuery → S3 tab.')
        except Exception as exc:
            st.error(str(exc))
    queue = st.session_state.get('job_queue', [])
    if queue and queue[0].get('source_type') == 'bigquery':
        st.dataframe([{'Source': j['id'], 'Format': j['file_format'], 'S3 key': j['s3_key'],
                       'Status': j['status'], 'Error': j['error']} for j in queue], hide_index=True)


def render_export(s3_cfg, rs_cfg):
    from chunk_02_job_queue_engine import render_execution_controls

    st.markdown('### BigQuery export')
    if not st.session_state.get('bq_connected'):
        st.info('Connect BigQuery in the sidebar first.')
        return
    _selection_form('export', s3_cfg, rs_cfg, execute=True)
    with st.expander('Advanced — Job Queue Engine'):
        render_execution_controls({'client': st.session_state.bq_client}, s3_cfg, rs_cfg)
