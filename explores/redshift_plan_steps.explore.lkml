include: "/views/*.view"
explore: redshift_plan_steps {
  join: redshift_tables {
    sql_on: ${redshift_tables.table}=${redshift_plan_steps.table} ;;
    type: left_outer
    relationship: many_to_one
  }
  join: redshift_queries {
    sql_on: ${redshift_queries.query} = ${redshift_plan_steps.query} ;;
    relationship: many_to_one
    type: left_outer
  }
  join: inner_child {
    from: redshift_plan_steps
    view_label: "Redshift Plan Steps > Inner Child"
    sql_on: ${inner_child.query}=${redshift_plan_steps.query}
      AND   ${inner_child.parent_step} = ${redshift_plan_steps.step}
      AND   ${inner_child.inner_outer} = 'inner'
      AND   ${inner_child.parent_step} <> 0;;
    type: left_outer
    relationship: one_to_one
    fields: [table,rows,bytes,total_rows,total_bytes]
  }
  join: outer_child {
    from: redshift_plan_steps
    view_label: "Redshift Plan Steps > Outer Child"
    sql_on: ${outer_child.query}=${redshift_plan_steps.query}
      AND   ${outer_child.parent_step} = ${redshift_plan_steps.step}
      AND   ${outer_child.inner_outer} = 'outer'
      AND   ${outer_child.parent_step} <> 0;;
    type: left_outer
    relationship: one_to_one
    fields: [table,rows,bytes,total_rows,total_bytes]
  }
  join: next_1 {
    from: redshift_plan_steps
    view_label: "Redshift Plan Steps > Parent 1 Operation"
    sql_on: ${next_1.query}=${redshift_plan_steps.query}
      AND ${next_1.step}=${redshift_plan_steps.parent_step}
      AND ${redshift_plan_steps.parent_step}<>0;;
    type: left_outer
    relationship: one_to_one
    fields: [operation,operation_argument,rows]
  }
  join: next_2 {
    from: redshift_plan_steps
    view_label: "Redshift Plan Steps > Parent 2 Operation"
    sql_on: ${next_2.query}=${next_1.query}
      AND ${next_2.step}=${next_1.parent_step}
      AND ${next_1.parent_step}<>0;;
    type: left_outer
    relationship: one_to_one
    fields: [operation,operation_argument,rows]
  }
}
