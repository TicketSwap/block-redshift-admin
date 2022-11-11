- dashboard: redshift_query_inspection
  preferred_viewer: dashboards-next
  title: Redshift Query Inspection
  layout: newspaper
  elements:
  - title: Time Executing
    name: Time Executing
    model: block_redshift_admin_v2
    explore: redshift_queries
    type: single_value
    fields: [redshift_queries.total_time_executing]
    limit: 500
    column_limit: 50
    custom_color_enabled: false
    custom_color: forestgreen
    show_single_value_title: true
    show_comparison: false
    comparison_type: value
    comparison_reverse_colors: false
    show_comparison_label: true
    stacking: ''
    show_value_labels: false
    label_density: 25
    legend_position: center
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: true
    limit_displayed_rows: false
    y_axis_combined: true
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: true
    show_x_axis_ticks: true
    x_axis_scale: auto
    y_axis_scale_mode: linear
    ordering: none
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    series_types: {}
    single_value_title: seconds to run
    value_format: "#,##0.0"
    listen:
      query: redshift_queries.query
    row: 0
    col: 0
    width: 8
    height: 3
  - title: Bytes Scanned
    name: Bytes Scanned
    model: block_redshift_admin_v2
    explore: redshift_query_execution
    type: single_value
    fields: [redshift_query_execution.total_bytes_broadcast, redshift_query_execution.total_bytes_distributed,
      redshift_query_execution.total_bytes_scanned, redshift_query_execution.total_rows_sorted_approx,
      redshift_query_execution.average_step_skew, redshift_query_execution.any_disk_based]
    limit: 500
    column_limit: 50
    query_timezone: America/Los_Angeles
    custom_color_enabled: false
    custom_color: forestgreen
    show_single_value_title: true
    show_comparison: false
    comparison_type: value
    comparison_reverse_colors: false
    show_comparison_label: true
    stacking: ''
    show_value_labels: false
    label_density: 25
    legend_position: center
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: true
    limit_displayed_rows: false
    y_axis_combined: true
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: true
    show_x_axis_ticks: true
    x_axis_scale: auto
    y_axis_scale_mode: linear
    ordering: none
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    series_types: {}
    value_format: '#,##0.0,," Mb"'
    single_value_title: Scanned
    hidden_fields: [redshift_query_execution.total_bytes_broadcast, redshift_query_execution.total_bytes_distributed]
    listen:
      query: redshift_query_execution.query
    row: 0
    col: 8
    width: 8
    height: 3
  - title: Query text
    name: Query text
    model: block_redshift_admin_v2
    explore: redshift_queries
    type: table
    fields: [redshift_queries.text]
    sorts: [redshift_queries.text]
    limit: 500
    column_limit: 50
    query_timezone: America/Los_Angeles
    show_view_names: false
    show_row_numbers: false
    truncate_column_names: true
    hide_totals: false
    hide_row_totals: false
    table_theme: white
    limit_displayed_rows: true
    stacking: ''
    show_value_labels: false
    label_density: 25
    legend_position: center
    x_axis_gridlines: false
    y_axis_gridlines: true
    y_axis_combined: true
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: true
    show_x_axis_ticks: true
    x_axis_scale: auto
    y_axis_scale_mode: linear
    ordering: none
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    series_types: {}
    limit_displayed_rows_values:
      show_hide: show
      first_last: first
      num_rows: '1'
    listen:
      query: redshift_queries.query
    row: 6
    col: 0
    width: 24
    height: 6
  - title: Bytes Distributed
    name: Bytes Distributed
    model: block_redshift_admin_v2
    explore: redshift_query_execution
    type: single_value
    fields: [redshift_query_execution.total_bytes_broadcast, redshift_query_execution.total_bytes_distributed,
      redshift_query_execution.total_bytes_scanned, redshift_query_execution.total_rows_sorted_approx,
      redshift_query_execution.average_step_skew, redshift_query_execution.any_disk_based]
    limit: 500
    column_limit: 50
    custom_color_enabled: false
    custom_color: forestgreen
    show_single_value_title: true
    show_comparison: false
    comparison_type: value
    comparison_reverse_colors: false
    show_comparison_label: true
    stacking: ''
    show_value_labels: false
    label_density: 25
    legend_position: center
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: true
    limit_displayed_rows: false
    y_axis_combined: true
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: true
    show_x_axis_ticks: true
    x_axis_scale: auto
    y_axis_scale_mode: linear
    ordering: none
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    series_types: {}
    value_format: '#,##0.0,," Mb"'
    single_value_title: Distributed
    hidden_fields: [redshift_query_execution.total_bytes_broadcast]
    listen:
      query: redshift_query_execution.query
    row: 0
    col: 16
    width: 8
    height: 3
  - title: Bytes Broadcast
    name: Bytes Broadcast
    model: block_redshift_admin_v2
    explore: redshift_query_execution
    type: single_value
    fields: [redshift_query_execution.total_bytes_broadcast, redshift_query_execution.total_bytes_distributed,
      redshift_query_execution.total_bytes_scanned, redshift_query_execution.total_rows_sorted_approx,
      redshift_query_execution.average_step_skew, redshift_query_execution.any_disk_based]
    limit: 500
    column_limit: 50
    query_timezone: America/Los_Angeles
    custom_color_enabled: false
    custom_color: forestgreen
    show_single_value_title: true
    show_comparison: false
    comparison_type: value
    comparison_reverse_colors: false
    show_comparison_label: true
    stacking: ''
    show_value_labels: false
    label_density: 25
    legend_position: center
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: true
    limit_displayed_rows: false
    y_axis_combined: true
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: true
    show_x_axis_ticks: true
    x_axis_scale: auto
    y_axis_scale_mode: linear
    ordering: none
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    series_types: {}
    value_format: '#,##0.0,," Mb"'
    single_value_title: Broadcast
    listen:
      query: redshift_query_execution.query
    row: 3
    col: 0
    width: 8
    height: 3
  - title: Rows Sorted
    name: Rows Sorted
    model: block_redshift_admin_v2
    explore: redshift_query_execution
    type: single_value
    fields: [redshift_query_execution.total_bytes_broadcast, redshift_query_execution.total_bytes_distributed,
      redshift_query_execution.total_bytes_scanned, redshift_query_execution.total_rows_sorted_approx,
      redshift_query_execution.average_step_skew, redshift_query_execution.any_disk_based]
    limit: 500
    column_limit: 50
    query_timezone: America/Los_Angeles
    custom_color_enabled: false
    custom_color: forestgreen
    show_single_value_title: true
    show_comparison: false
    comparison_type: value
    comparison_reverse_colors: false
    show_comparison_label: true
    stacking: ''
    show_value_labels: false
    label_density: 25
    legend_position: center
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: true
    limit_displayed_rows: false
    y_axis_combined: true
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: true
    show_x_axis_ticks: true
    x_axis_scale: auto
    y_axis_scale_mode: linear
    ordering: none
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    series_types: {}
    value_format: '#,##0, "k"'
    single_value_title: Rows sorted
    hidden_fields: [redshift_query_execution.total_bytes_broadcast, redshift_query_execution.total_bytes_distributed,
      redshift_query_execution.total_bytes_scanned]
    listen:
      query: redshift_query_execution.query
    row: 3
    col: 8
    width: 8
    height: 3
  - title: Was Disk Based
    name: Was Disk Based
    model: block_redshift_admin_v2
    explore: redshift_query_execution
    type: single_value
    fields: [redshift_query_execution.total_bytes_broadcast, redshift_query_execution.total_bytes_distributed,
      redshift_query_execution.total_bytes_scanned, redshift_query_execution.total_rows_sorted_approx,
      redshift_query_execution.average_step_skew, redshift_query_execution.any_disk_based]
    limit: 500
    column_limit: 50
    query_timezone: America/Los_Angeles
    custom_color_enabled: false
    custom_color: forestgreen
    show_single_value_title: true
    show_comparison: false
    comparison_type: value
    comparison_reverse_colors: false
    show_comparison_label: true
    stacking: ''
    show_value_labels: false
    label_density: 25
    legend_position: center
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: true
    limit_displayed_rows: false
    y_axis_combined: true
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: true
    show_x_axis_ticks: true
    x_axis_scale: auto
    y_axis_scale_mode: linear
    ordering: none
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    series_types: {}
    value_format: ''
    single_value_title: Disk based?
    hidden_fields: [redshift_query_execution.total_bytes_broadcast, redshift_query_execution.total_bytes_distributed,
      redshift_query_execution.total_bytes_scanned, redshift_query_execution.total_rows_sorted_approx,
      redshift_query_execution.average_step_skew]
    listen:
      query: redshift_query_execution.query
    row: 3
    col: 16
    width: 8
    height: 3
  - title: Table Details
    name: Table Details
    model: block_redshift_admin_v2
    explore: redshift_tables
    type: table
    fields: [redshift_tables.schema, redshift_tables.table, redshift_tables.rows_in_table,
      redshift_tables.distribution_style, redshift_tables.skew_rows, redshift_tables.encoded,
      redshift_tables.sortkey, redshift_tables.sortkey_encoding, redshift_tables.unsorted,
      redshift_tables.stats_off, redshift_query_execution.total_bytes_scanned, redshift_query_execution.emitted_rows_to_table_rows_ratio,
      redshift_query_execution.any_restricted_scan]
    sorts: [redshift_query_execution.total_bytes_scanned desc]
    limit: 40
    column_limit: 50
    query_timezone: America/Los_Angeles
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
    stacking: ''
    show_value_labels: false
    label_density: 25
    legend_position: center
    x_axis_gridlines: false
    y_axis_gridlines: true
    y_axis_combined: true
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: true
    show_x_axis_ticks: true
    x_axis_scale: auto
    y_axis_scale_mode: linear
    ordering: none
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    series_types: {}
    listen:
      query: redshift_queries.query
    row: 12
    col: 0
    width: 24
    height: 6
  - title: Query Plan Costs
    name: Query Plan Costs
    model: block_redshift_admin_v2
    explore: redshift_plan_steps
    type: table
    fields: [redshift_plan_steps.parent_step, redshift_plan_steps.step, redshift_plan_steps.step_description,
      redshift_plan_steps.step_cost]
    sorts: [redshift_plan_steps.step]
    limit: 2000
    column_limit: 50
    query_timezone: America/Los_Angeles
    show_view_names: false
    show_row_numbers: true
    truncate_column_names: false
    hide_totals: false
    hide_row_totals: false
    table_theme: gray
    limit_displayed_rows: false
    enable_conditional_formatting: false
    conditional_formatting_include_totals: false
    conditional_formatting_include_nulls: false
    diameter: 100%
    stepwise_max_scale: 4
    series_types: {}
    listen:
      query: redshift_plan_steps.query
    row: 18
    col: 0
    width: 24
    height: 11
  - title: Query Plan
    name: Query Plan
    model: block_redshift_admin_v2
    explore: redshift_plan_steps
    type: table
    fields: [redshift_plan_steps.step, redshift_plan_steps.parent_step, redshift_plan_steps.operation,
      redshift_plan_steps.network_distribution_type, redshift_plan_steps.operation_argument,
      redshift_plan_steps.table, redshift_plan_steps.rows, redshift_plan_steps.bytes]
    sorts: [redshift_plan_steps.step]
    limit: 2000
    column_limit: 50
    query_timezone: America/Los_Angeles
    show_view_names: false
    show_row_numbers: true
    truncate_column_names: false
    hide_totals: false
    hide_row_totals: false
    table_theme: gray
    limit_displayed_rows: false
    enable_conditional_formatting: false
    conditional_formatting_include_totals: false
    conditional_formatting_include_nulls: false
    listen:
      query: redshift_plan_steps.query
    row: 29
    col: 0
    width: 24
    height: 11
  - title: Query Execution
    name: Query Execution
    model: block_redshift_admin_v2
    explore: redshift_query_execution
    type: table
    fields: [redshift_query_execution.step, redshift_query_execution.label, redshift_query_execution.was_diskbased,
      redshift_query_execution.rows_out, redshift_query_execution.bytes, redshift_query_execution.step_skew,
      redshift_query_execution.step_max_slice_time]
    sorts: [redshift_query_execution.step]
    limit: 500
    column_limit: 50
    query_timezone: America/Los_Angeles
    show_view_names: false
    show_row_numbers: true
    truncate_column_names: false
    hide_totals: false
    hide_row_totals: false
    table_theme: gray
    limit_displayed_rows: false
    enable_conditional_formatting: false
    conditional_formatting_include_totals: false
    conditional_formatting_include_nulls: false
    listen:
      query: redshift_query_execution.query
    row: 40
    col: 0
    width: 24
    height: 13
  filters:
  - name: query
    title: Query
    type: number_filter
    default_value:
    allow_multiple_values: true
    required: false
