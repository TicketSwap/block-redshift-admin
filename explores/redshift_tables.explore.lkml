include: "/views/*.view"
explore: redshift_tables {
  hidden: yes
  view_label: "[Redshift Tables]"
  join: redshift_query_execution {
    sql_on: ${redshift_query_execution.table_join_key}=${redshift_tables.table_join_key};;
    relationship: one_to_many
    type: left_outer
    fields: [
      any_restricted_scan,
      count_scans,
      percent_restricted_scan,
      total_bytes_scanned,
      total_rows_emitted,
      emitted_rows_to_table_rows_ratio
    ]
  }
  join: redshift_queries {
    sql_on: ${redshift_queries.query} = ${redshift_query_execution.query} ;;
    relationship: many_to_one
    type: left_outer
    fields: [query,start_date, time_executing, snippet, pdt, count,total_time_executing,time_executing_per_query]
  }
}
