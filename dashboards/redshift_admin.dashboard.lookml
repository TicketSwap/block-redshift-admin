- dashboard: redshift_admin
  preferred_viewer: dashboards-next
  title: Redshift Admin
  layout: newspaper
  elements:
  - title: Table Load Summary
    name: Table Load Summary
    model: block_redshift_admin_v2
    explore: redshift_data_loads
    type: table
    fields: [redshift_data_loads.root_bucket, redshift_data_loads.s3_path_clean, redshift_data_loads.file_stem,
      redshift_data_loads.hours_since_last_load]
    sorts: [redshift_data_loads.root_bucket]
    limit: 500
    show_view_names: true
    show_row_numbers: true
    truncate_column_names: false
    hide_totals: false
    hide_row_totals: false
    table_theme: gray
    limit_displayed_rows: false
    enable_conditional_formatting: false
    conditional_formatting_include_totals: false
    conditional_formatting_include_nulls: false
    listen: {}
    row: 0
    col: 2
    width: 20
    height: 13
  - title: Recent Files Loaded
    name: Recent Files Loaded
    model: block_redshift_admin_v2
    explore: redshift_data_loads
    type: table
    fields: [redshift_data_loads.file_name, redshift_data_loads.hours_since_last_load]
    filters:
      redshift_data_loads.load_date: 3 hours
    sorts: [redshift_data_loads.hours_since_last_load]
    limit: 500
    show_view_names: true
    show_row_numbers: true
    truncate_column_names: false
    hide_totals: false
    hide_row_totals: false
    table_theme: gray
    limit_displayed_rows: false
    enable_conditional_formatting: false
    conditional_formatting_include_totals: false
    conditional_formatting_include_nulls: false
    listen: {}
    row: 26
    col: 2
    width: 20
    height: 14
  - title: Recent Load Errors
    name: Recent Load Errors
    model: block_redshift_admin_v2
    explore: redshift_etl_errors
    type: table
    fields: [redshift_etl_errors.error_date, redshift_etl_errors.file_name, redshift_etl_errors.column_name,
      redshift_etl_errors.column_data_type, redshift_etl_errors.error_reason]
    filters:
      redshift_etl_errors.error_date: 7 days
    sorts: [redshift_etl_errors.error_date desc]
    limit: 500
    show_view_names: true
    show_row_numbers: true
    truncate_column_names: false
    hide_totals: false
    hide_row_totals: false
    table_theme: gray
    limit_displayed_rows: false
    enable_conditional_formatting: false
    conditional_formatting_include_totals: false
    conditional_formatting_include_nulls: false
    listen: {}
    row: 40
    col: 2
    width: 20
    height: 13
  - title: Database Consumption
    name: Database Consumption
    model: block_redshift_admin_v2
    explore: redshift_db_space
    type: table
    fields: [redshift_db_space.schema, redshift_db_space.table_stem, redshift_db_space.total_rows,
      redshift_db_space.total_megabytes, redshift_db_space.total_tables]
    sorts: [redshift_db_space.total_megabytes desc]
    limit: 500
    show_view_names: true
    show_row_numbers: true
    truncate_column_names: false
    hide_totals: false
    hide_row_totals: false
    table_theme: gray
    limit_displayed_rows: false
    enable_conditional_formatting: false
    conditional_formatting_include_totals: false
    conditional_formatting_include_nulls: false
    listen: {}
    row: 13
    col: 2
    width: 20
    height: 13
