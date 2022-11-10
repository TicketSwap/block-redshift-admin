view: redshift_queries_avg {
    derived_table: {
      explore_source: redshift_queries {
        column: query {}
        column: avg_time_executing {}
      }
    }

    # PARAMETERS #

    parameter: dynamic_tier_size {
      default_value: "5"
      type: number
    }

    # DIMENSIONS #

    dimension: query {
      primary_key: yes
      description: "Redshift's Query ID"
      value_format: "0"
      type: number
    }

    dimension: avg_time_executing {
      description: "Average time that queries were executing, in seconds"
      value_format: "#,##0.0"
      type: number
      sql: ${TABLE}.avg_time_executing::float ;;
    }

    dimension: avg_time_executing_tier_static {
      type: tier
      tiers: [0,5,10,20,30,40,50,60,90,120,180,240,300]
      sql: ${avg_time_executing} ;;
      style: interval
    }

    dimension: avg_time_executing_dynamic  {
      description: "Use with 'Dynamic Tier Size'"
      sql:
        (ROUND(${avg_time_executing}/{% parameter dynamic_tier_size %}, 0) * {% parameter dynamic_tier_size %})::varchar
        || ' - ' ||
        (ROUND(${avg_time_executing}/{% parameter dynamic_tier_size %}, 0) * {% parameter dynamic_tier_size %} + {% parameter dynamic_tier_size %})::varchar
      ;;
      order_by_field: dynamic_sort_field
    }

    dimension: dynamic_sort_field {
      sql:
        ROUND(${avg_time_executing}/{% parameter dynamic_tier_size %}, 0) * {% parameter dynamic_tier_size %};;
      type: number
      hidden: yes
  }

  # MEASURES #

    measure: count {
      type: count
    }
  }
