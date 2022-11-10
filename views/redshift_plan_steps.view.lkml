view: redshift_plan_steps {
  derived_table: {
    datagroup_trigger: nightly
    distribution: "query"
    sortkeys: ["query"]
    sql:
        WITH redshift_plan_steps AS
          (SELECT
            query, nodeid, parentid,
            CASE WHEN plannode='SubPlan' THEN 'SubPlan'
            ELSE substring(regexp_substr(plannode, 'XN( [A-Z][a-z]+)+'),4) END as operation,
            substring(regexp_substr(plannode, 'DS_[A-Z_]+'),0) as network_distribution_type,
            substring(info from 1 for 240) as operation_argument,
            CASE
              WHEN plannode NOT LIKE '% on %' THEN NULL
              WHEN plannode LIKE '% on "%' THEN substring(regexp_substr(plannode,' on "[^"]+'),6)
              ELSE substring(regexp_substr(plannode,' on [\._a-zA-Z0-9]+'),5)
            END as "table",
            RIGHT('0'||COALESCE(substring(regexp_substr(plannode,' rows=[0-9]+'),7),''),32)::decimal(38,0) as "rows",
            RIGHT('0'||COALESCE(substring(regexp_substr(plannode,' width=[0-9]+'),8),''),32)::decimal(38,0) as width,
            substring(regexp_substr(plannode,'\\(cost=[0-9]+'),7) as cost_lo_raw,
            substring(regexp_substr(plannode,'\\.\\.[0-9]+'),3) as cost_hi_raw,
            CASE
              WHEN cost_hi_raw != '' THEN
                ( CASE WHEN LEN(cost_hi_raw) > 18 THEN 999999999999999999::DECIMAL(38,0) ELSE cost_hi_raw::DECIMAL(38,0) END )
              ELSE NULL END  as cost_hi_numeric,
            CASE
              WHEN COALESCE(parentid,0)=0 THEN 'root'
              WHEN nodeid = MAX(nodeid) OVER (PARTITION BY query,parentid) THEN 'inner'
              ELSE 'outer' END::CHAR(5) as inner_outer,
            SUM(cost_hi_numeric) OVER (PARTITION by query,parentid) as sum_children_cost
          FROM stl_explain
          WHERE query>=(SELECT min(query) FROM ${redshift_queries.SQL_TABLE_NAME})
            AND query<=(SELECT max(query) FROM ${redshift_queries.SQL_TABLE_NAME})
          GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12)
        SELECT
          redshift_plan_steps.*,
          redshift_plan_steps.cost_hi_numeric - COALESCE(x.sum_children_cost,0) AS incremental_step_cost,
          ROW_NUMBER() OVER () as pk
        FROM redshift_plan_steps
        LEFT JOIN (SELECT query, parentid, sum_children_cost FROM redshift_plan_steps GROUP BY 1,2,3) AS x
          ON redshift_plan_steps.query = x.query AND redshift_plan_steps.nodeid = x.parentid
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
