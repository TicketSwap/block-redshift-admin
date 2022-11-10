include: "/views/*.view"
explore: redshift_query_execution {
  hidden: yes
  persist_for: "0 seconds"
  fields: [ALL_FIELDS*, -redshift_query_execution.emitted_rows_to_table_rows_ratio]
}
