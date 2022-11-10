include: "/views/*.view"
explore: redshift_queries {
  persist_for: "0 seconds"
  join: redshift_queries_avg {
    relationship: many_to_one
    sql_on: ${redshift_queries.query} = ${redshift_queries_avg.query} ;;
  }
}
