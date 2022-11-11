- dashboard: redshift_performance
  preferred_viewer: dashboards-next
  title: Redshift Performance
  layout: newspaper
  elements:
  - name: Modeling
    type: text
    title_text: Modeling
    subtitle_text: ''
    body_text: ''
    row: 6
    col: 0
    width: 24
    height: 2
  - name: Capacity
    type: text
    title_text: Capacity
    subtitle_text: ''
    body_text: ''
    row: 14
    col: 0
    width: 24
    height: 2
  - title: Top Network Distribution Operations
    name: Top Network Distribution Operations
    model: block_redshift_admin_v2
    explore: redshift_plan_steps
    type: table
    fields: [redshift_plan_steps.network_distribution_type, redshift_plan_steps.operation_argument,
      redshift_queries.count, redshift_queries.total_time_executing, redshift_queries.time_executing_per_query]
    filters:
      redshift_plan_steps.network_distribution_type: '"DS_DIST_OUTER","DS_DIST_ALL_INNER","DS_DIST_BOTH","DS_BCAST_INNER"'
      redshift_plan_steps.operation: "%Join%"
    sorts: [redshift_queries.total_time_executing desc]
    limit: 50
    column_limit: 50
    show_view_names: false
    show_row_numbers: true
    truncate_column_names: false
    hide_totals: false
    hide_row_totals: false
    table_theme: gray
    limit_displayed_rows: true
    limit_displayed_rows_values:
      show_hide: show
      first_last: first
      num_rows: '20'
    enable_conditional_formatting: false
    conditional_formatting_include_totals: false
    conditional_formatting_include_nulls: false
    value_labels: labels
    label_type: labPer
    series_types: {}
    series_colors:
      DS_BCAST_INNER: "#d3271d"
      DS_DIST_BOTH: "#fa6600"
      DS_DIST_INNER: "#82c400"
      DS_DIST_NONE: "#276300"
      DS_DIST_ALL_INNER: "#5f00cf"
      DS_DIST_ALL_NONE: "#1c8b19"
    listen:
      PDT: redshift_queries.pdt
    row: 8
    col: 12
    width: 12
    height: 6
  - title: Query Count By Run Time Tier
    name: Query Count By Run Time Tier
    model: block_redshift_admin_v2
    explore: redshift_queries
    type: looker_column
    fields: [redshift_queries.count, redshift_queries.time_executing_tier]
    filters:
      redshift_queries.time_executing: not 0
    sorts: [redshift_queries.time_executing_tier]
    limit: 500
    column_limit: 50
    color_application:
      collection_id: b43731d5-dc87-4a8e-b807-635bef3948e7
      palette_id: ccba75a3-58c7-4b9c-a931-4ffc59e79cba
      options:
        steps: 5
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: false
    y_axes: [{label: '', orientation: left, series: [{axisId: redshift_queries.count,
            id: redshift_queries.count, name: Redshift Queries Count}], showLabels: false,
        showValues: false, unpinAxis: false, tickDensity: default, tickDensityCustom: 5,
        type: linear}]
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: true
    show_x_axis_ticks: true
    y_axis_scale_mode: log
    x_axis_reversed: false
    y_axis_reversed: false
    plot_size_by_field: false
    trellis: ''
    stacking: ''
    limit_displayed_rows: false
    legend_position: center
    series_types: {}
    point_style: none
    series_colors: {}
    show_value_labels: true
    label_density: 25
    x_axis_scale: auto
    y_axis_combined: true
    ordering: none
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    show_null_points: true
    interpolation: linear
    listen:
      PDT: redshift_queries.pdt
    row: 0
    col: 0
    width: 12
    height: 6
  - title: Top 10 Longest Running Queries
    name: Top 10 Longest Running Queries
    model: block_redshift_admin_v2
    explore: redshift_queries
    type: looker_bar
    fields: [redshift_queries.query, redshift_queries.snippet, redshift_queries.total_time_executing]
    sorts: [redshift_queries.total_time_executing desc]
    limit: 10
    column_limit: 50
    color_application:
      collection_id: b43731d5-dc87-4a8e-b807-635bef3948e7
      palette_id: ccba75a3-58c7-4b9c-a931-4ffc59e79cba
      options:
        steps: 5
    x_axis_gridlines: false
    y_axis_gridlines: false
    show_view_names: false
    y_axes: [{label: '', orientation: bottom, series: [{axisId: redshift_queries.total_time_executing,
            id: redshift_queries.total_time_executing, name: Total Time Executing}],
        showLabels: false, showValues: false, unpinAxis: false, tickDensity: default,
        tickDensityCustom: 5, type: linear}]
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: false
    show_x_axis_ticks: true
    y_axis_scale_mode: linear
    x_axis_reversed: false
    y_axis_reversed: false
    plot_size_by_field: false
    trellis: ''
    stacking: ''
    limit_displayed_rows: false
    legend_position: center
    series_types: {}
    point_style: none
    series_colors:
      redshift_queries.total_time_executing: "#592EC2"
    series_labels:
      redshift_queries.time_executing_roundup1: Run time (seconds)
      redshift_queries.query: Query ID
    show_value_labels: true
    label_density: 25
    x_axis_scale: auto
    y_axis_combined: true
    ordering: none
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    show_row_numbers: true
    truncate_column_names: false
    hide_totals: false
    hide_row_totals: false
    table_theme: editable
    hidden_fields: [redshift_queries.snippet]
    listen:
      PDT: redshift_queries.pdt
    row: 0
    col: 12
    width: 12
    height: 6
  - title: Network distribution breakdown
    name: Network distribution breakdown
    model: block_redshift_admin_v2
    explore: redshift_plan_steps
    type: looker_pie
    fields: [redshift_plan_steps.network_distribution_type, redshift_queries.total_time_executing]
    filters:
      redshift_plan_steps.operation: "%Join%"
    sorts: [redshift_queries.total_time_executing desc]
    limit: 500
    column_limit: 50
    value_labels: labels
    label_type: labPer
    color_application:
      collection_id: b43731d5-dc87-4a8e-b807-635bef3948e7
      palette_id: fb7bb53e-b77b-4ab6-8274-9d420d3d73f3
      options:
        steps: 5
    series_colors: {}
    show_view_names: false
    show_row_numbers: true
    truncate_column_names: false
    hide_totals: false
    hide_row_totals: false
    table_theme: editable
    limit_displayed_rows: false
    series_types: {}
    listen:
      PDT: redshift_queries.pdt
    row: 8
    col: 0
    width: 12
    height: 6
  - title: Queries Submitted & Queued by Hour (Last 4 Weeks)
    name: Queries Submitted & Queued by Hour (Last 4 Weeks)
    model: block_redshift_admin_v2
    explore: redshift_queries
    type: looker_line
    fields: [redshift_queries.start_hour, redshift_queries.count, redshift_queries.count_of_queued,
      redshift_queries.percent_queued, redshift_queries.total_time_in_queue]
    fill_fields: [redshift_queries.start_hour]
    filters:
      redshift_queries.start_date: 4 weeks
    sorts: [redshift_queries.start_hour desc]
    limit: 500
    column_limit: 50
    dynamic_fields: [{table_calculation: minutes_queued, label: Minutes Queued, expression: "${redshift_queries.total_time_in_queue}/60",
        value_format: !!null '', value_format_name: decimal_1}]
    color_application:
      collection_id: b43731d5-dc87-4a8e-b807-635bef3948e7
      palette_id: fb7bb53e-b77b-4ab6-8274-9d420d3d73f3
      options:
        steps: 5
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: false
    y_axes: [{label: Count, orientation: left, series: [{axisId: redshift_queries.count,
            id: redshift_queries.count, name: Redshift Queries}, {axisId: redshift_queries.count_of_queued,
            id: redshift_queries.count_of_queued, name: Count of Queued}, {axisId: minutes_queued,
            id: minutes_queued, name: Minutes Queued}], showLabels: false, showValues: true,
        unpinAxis: false, tickDensity: default, tickDensityCustom: 5, type: linear}]
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_labels: [Count, Queued, Minutes Queued]
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: false
    show_x_axis_ticks: true
    y_axis_scale_mode: linear
    x_axis_reversed: false
    y_axis_reversed: false
    plot_size_by_field: false
    trellis: ''
    stacking: ''
    limit_displayed_rows: false
    hidden_series: [minutes_queued]
    legend_position: center
    colors: ['palette: Tomato to Steel Blue']
    series_types:
      redshift_queries.count_of_queued: area
      minutes_queued: area
    point_style: none
    series_colors: {}
    show_value_labels: false
    label_density: 25
    x_axis_scale: auto
    y_axis_combined: true
    y_axis_orientation: [left, left, right]
    show_null_points: true
    interpolation: linear
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    ordering: none
    show_null_labels: false
    hidden_fields: [redshift_queries.total_time_in_queue, redshift_queries.percent_queued]
    listen:
      PDT: redshift_queries.pdt
    row: 16
    col: 0
    width: 24
    height: 6
  filters:
  - name: PDT
    title: PDT
    type: field_filter
    default_value: ''
    allow_multiple_values: true
    required: false
    model: block_redshift_admin_v2
    explore: redshift_queries
    listens_to_filters: []
    field: redshift_queries.pdt
