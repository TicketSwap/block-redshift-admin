view: redshift_plan_steps {
  derived_table: {
    datagroup_trigger: nightly
    distribution: "query"
    sortkeys: ["query"]
    sql:
        SELECT
          query,
          nodeid,
          parentid,
          operation,
          network_distribution_type,
          operation_argument,
          "table",
          rows,
          width,
          cost_lo_raw,
          cost_hi_raw,
          cost_hi_numeric,
          inner_outer,
          sum_children_cost,
          incremental_step_cost,
          ROW_NUMBER() OVER (ORDER BY query, starttime) as pk,
          starttime,
          endtime
        FROM history.hist_redshift_plan_steps_view
        ORDER BY 1,3
    ;;

    }

    # DIMENSIONS #

    dimension: pk {
      sql: ${TABLE}.pk ;;
      primary_key: yes
    }

    dimension: query {
      sql: ${TABLE}.query;;
      type: number
      value_format_name: id
      drill_fields: [redshift_plan_steps.step]
    }

    dimension: step {
      sql: ${TABLE}.nodeid ;;
      type: number
      value_format_name: id
    }

    dimension: query_step {
      sql: ${query}||'.'||${step} ;;
      hidden: yes
    }

    dimension: parent_step {
      type: number
      sql: ${TABLE}.parentid;;
      hidden: yes
    }

    dimension: step_description {
      description: "Concatenation of 'operation - network distribution type - table'"
      sql: CASE WHEN COALESCE(${operation},'') = '' THEN '' ELSE ${operation} END ||
           CASE WHEN COALESCE(${operation_argument},'') = '' THEN '' ELSE ' - ' || ${operation_argument} END ||
           CASE WHEN COALESCE(${network_distribution_type},'') = '' THEN '' ELSE ' - ' || ${network_distribution_type} END ||
           CASE WHEN COALESCE(${table},'') = '' THEN '' ELSE ' - ' || ${table} END ;;
      type: "string"
      hidden: yes
    }

    dimension: operation {
      label: "Operation"
      sql: ${TABLE}.operation ;;
      type: "string"
      html:
      {% if value contains 'Nested' %}
        <span style="color: darkred">{{ rendered_value }}</span>
      {% else %}
        {{ rendered_value }}
      {% endif %}
    ;;
    }

    dimension: operation_join_algorithm {
      type: "string"
      sql: CASE WHEN ${operation} ILIKE '%Join%'
              THEN regexp_substr(${operation},'^[A-Za-z]+')
              ELSE 'Not a Join' END
            ;;
      html:
      {% if value == 'Nested' %}
      <span style="color: darkred">{{ rendered_value }}</span>
      {% else %}
      {{ rendered_value }}
      {% endif %}
    ;;
    }

    dimension: network_distribution_type {
      label: "Network Redistribution"
      description: "AWS Docs http://docs.aws.amazon.com/redshift/latest/dg/c_data_redistribution.html"
      sql: ${TABLE}.network_distribution_type ;;
      type: "string"
      html: <span style="color: {% if
             value == 'DS_DIST_NONE' %} #37ce12 {% elsif
             value == 'DS_DIST_ALL_NONE' %} #17470c {% elsif
             value == 'DS_DIST_INNER' %} #5f7c58 {% elsif
             value == 'DS_DIST_OUTER' %} #ff8828 {% elsif
             value == 'DS_DIST_BOTH' %} #c13c07 {% elsif
             value == 'DS_BCAST_INNER' %} #d6a400 {% elsif
             value == 'DS_DIST_ALL_INNER' %} #9e0f62 {% else
            %} black {% endif %}">{{ rendered_value }}</span>
            ;;
    }

    dimension: network_distribution_bytes {
      description: "Bytes from inner and outer children needing to be distributed or broadcast. (For broadcast, this value does not multiply by the number of nodes broadcast to.)"
      sql: CASE
              WHEN ${network_distribution_type} ILIKE '%INNER%' THEN ${inner_child.bytes}
              WHEN ${network_distribution_type} ILIKE '%OUTER%' THEN ${outer_child.bytes}
              WHEN ${network_distribution_type} ILIKE '%BOTH%' THEN ${inner_child.bytes} + ${outer_child.bytes}
              ELSE 0
            END ;;
    }

    dimension: table {
      sql: ${TABLE}."table" ;;
      type: "string"
    }

    dimension: operation_argument {
      label: "Operation argument"
      sql: ${TABLE}.operation_argument ;;
      type: "string"
    }

    dimension: rows {
      label: "Rows out"
      sql: ${TABLE}.rows;;
      description: "Number of rows returned from this step"
      type: "number"
    }

    dimension: width {
      label: "Width out"
      sql: ${TABLE}.width;;
      description: "The estimated width of the average row, in bytes, that is returned from this step"
      type: "number"
    }

    dimension:bytes{
      label: "Bytes out"
      description: "Estimated bytes out from this step (rows * width)"
      sql: ${rows} * ${width} ;;
      type: "number"
    }

    dimension: inner_outer {
      label: "Child Inner/Outer"
      description: "If the step is a child of another step, whether it is the inner or outer child of the parent, e.g. for network redistribution in joins"
      type: "string"
      sql: ${TABLE}.inner_outer ;;
    }

    dimension: cost_lo_raw {
      description: "Cumulative relative cost of returning the first row for this step"
      type: string
      sql: ${TABLE}.cost_lo_raw ;;
      hidden: yes
    }

    dimension: cost_hi_raw {
      description: "Cumulative relative cost of completing this step"
      type: string
      sql: ${TABLE}.cost_hi_raw ;;
      hidden: yes
    }

    dimension: incremental_step_cost {
      description: "Incremental relative cost of completing this step"
      type: number
      sql: ${TABLE}.incremental_step_cost ;;
    }

    dimension: starttime {
      description: "Start time of the query in redshift"
      type: date
      sql: ${TABLE}.starttime ;;
    }

    dimension: endtime {
      description: "End time of the query in redshift"
      type: date
      sql: ${TABLE}.endtime ;;
    }

    # MEASURES #

    measure: count {
      type: count
      drill_fields: [query, parent_step, step, operation, operation_argument, network_distribution_type]
    }

    measure: step_cost {
      type: sum
      sql: ${incremental_step_cost} ;;
      description: "Relative cost of completing steps"
    }

    measure: total_rows{
      label: "Total rows out"
      type:  "sum"
      sql:  ${rows} ;;
      description: "Sum of rows returned across steps"
    }

    measure: total_bytes {
      label: "Total bytes out"
      type: "sum"
      sql:  ${bytes} ;;
    }

    measure: total_network_distribution_bytes {
      type: sum
      sql: ${network_distribution_bytes} ;;
    }

    # SETS #

    set: steps_drill {
      fields: [
        redshift_plan_steps.query,
        redshift_plan_steps.parent_step,
        redshift_plan_steps.step,
        redshift_plan_steps.operation,
        redshift_plan_steps.operation_argument,
        redshift_plan_steps.network_distribution_type,
        redshift_plan_steps.rows,
        redshift_plan_steps.width,
        redshift_plan_steps.bytes
      ]
    }
  }
