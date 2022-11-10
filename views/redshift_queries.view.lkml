view: redshift_queries {
  # Limited to last 24 hours of queries
  derived_table: {
    datagroup_trigger: nightly
    distribution: "query"
    sortkeys: ["query"]
    sql: SELECT DISTINCT
        wlm.query,
        COALESCE(qlong.querytxt,q.substring)::varchar as text,
        SUBSTRING(
          REGEXP_REPLACE(COALESCE(qlong.querytxt,q.substring)::varchar,'^\\s*-- ([A-Za-z ]*''\\{[^}]*\\}''.|Building [^ ]+( in dev mode)? on instance [0-9a-f]+.)','')
          ,1,100) as snippet,
        CASE
          WHEN COALESCE(qlong.querytxt,q.substring)::varchar NOT ILIKE '-- Building %'
          THEN 'No'
          WHEN COALESCE(qlong.querytxt,q.substring)::varchar ILIKE '% in dev mode on instance %'
          THEN 'Dev'
          ELSE 'Prod'
          END as pdt,
        REGEXP_SUBSTR(COALESCE(qlong.querytxt,q.substring)::varchar,'^\\s*-- [A-Za-z ]*''\\{[^}]*"user_id":(\\d+)',1,1,'e') as looker_user_id,
        REGEXP_SUBSTR(COALESCE(qlong.querytxt,q.substring)::varchar,'^\\s*-- [A-Za-z ]*''\\{[^}]*"history_id":(\\d+)',1,1,'e') as looker_history_id,
        REGEXP_SUBSTR(COALESCE(qlong.querytxt,q.substring)::varchar,'^\\s*-- [^}]*instance_?s?l?u?g?"?:?"? ?([0-9a-f]+)',1,1,'e') as looker_instance_slug,
        sc.name as service_class,
        --wlm.service_class as service_class, --Use if connection was not given access to STV_WLM_SERVICE_CLASS_CONFIG
        wlm.service_class_start_time as start_time,
        wlm.total_queue_time,
        wlm.total_exec_time,
        q.elapsed, --Hmm.. this measure seems to be greater than queue_time+exec_time,
        ROW_NUMBER() OVER () AS pk
      FROM STL_WLM_QUERY wlm
      LEFT JOIN STV_WLM_SERVICE_CLASS_CONFIG sc ON sc.service_class=wlm.service_class -- Remove this line if access was not granted
      LEFT JOIN SVL_QLOG q on q.query=wlm.query
      LEFT JOIN STL_QUERY qlong on qlong.query=q.query
      WHERE wlm.service_class_start_time >= dateadd(day,-1,GETDATE())
      AND wlm.service_class_start_time <= GETDATE()
    ;;
    #STL_QUERY vs SVL_QLOG. STL_QUERY has more characters of query text (4000), but is only retained for "2 to 5 days"
    # STL_WLM_QUERY or SVL_QUERY_QUEUE_INFO? http://docs.aws.amazon.com/redshift/latest/dg/r_SVL_QUERY_QUEUE_INFO.html
    }

    # DIMENSIONS #

    dimension: pk {
      primary_key: yes
      hidden:  yes
      sql: ${TABLE}.pk ;;
    }

    #Looker Query Context '{"user_id":711,"history_id":38026310,"instance_slug":"186fb89f0c23199fffd36f1cdfb6152b"}
    dimension: query {
      description: "Redshift's Query ID"
      type: number
      value_format: "0"
      link: {
        label: "Inspect"
        url: "/dashboards/block_redshift_admin_v2::redshift_query_inspection?query={{value}}"
      }
    }

    dimension: text {
      alias: [querytxt]
    }

    dimension: snippet {
      alias: [substring]
    }

    dimension: looker_user_id {
      group_label: "Looker Query Context"
      type: number
      link: {
        label: "View in Looker Admin"
        url: "/admin/users/{{value}}/edit"
        # ^ Note that in scenarios with multiple Looker instances, this may not be the right link!
      }
    }

    dimension: looker_history_id {
      group_label: "Looker Query Context"
      type: number
      link: {
        label: "View in Looker Admin"
        url: "/admin/queries/{{value}}"
        # ^ Note that in scenarios with multiple Looker instances, this may not be the right link!
      }
    }

    dimension: looker_instance_slug {
      group_label: "Looker Query Context"
    }

    dimension: pdt {
      label: "PDT Type"
      group_label: "Looker Query Context"
      description: "Either Prod, Dev, or No"
      case: {
        when: {
          label: "Prod"
          sql: ${TABLE}.pdt = 'Prod' ;;
        }
        when: {
          label: "Dev"
          sql: ${TABLE}.pdt = 'Dev' ;;
        }
        when: {
          label: "Not a PDT"
          sql: ${TABLE}.pdt = 'No' ;;
        }
      }
    }

    dimension_group: start {
      type: time
      timeframes: [raw, minute,second, minute15, hour, hour_of_day, day_of_week, date]
      sql: ${TABLE}.start_time ;;
    }

    dimension: service_class {
      type: string
      sql: ${TABLE}.service_class ;;
    }

    dimension: time_in_queue {
      type: number
      description: "Amount of time that a query was queued before running, in seconds"
      sql: ${TABLE}.total_queue_time /1000000;;
    }

    dimension: time_executing {
      type: number
      description: "Amount of time that a query was executing, in seconds"
      sql: ${TABLE}.total_exec_time::float /1000000;;
    }

    dimension: time_executing_roundup1 {
      description: "Time executing, rounded up to the nearest 1 second"
      group_label: "Time Executing Buckets"
      label: "01 second"
      type: number
      sql: CEILING(${TABLE}.total_exec_time::float/1000000) ;;
      value_format_name: decimal_0
    }

    dimension: time_executing_roundup5 {
      description: "Time executing, rounded up to the nearest 5 seconds"
      group_label: "Time Executing Buckets"
      label: "5 seconds"
      type: number
      sql: CEILING(${TABLE}.total_exec_time::float/1000000 / 5)*5 ;;
      value_format_name: decimal_0
    }

    dimension: time_executing_tier {
      description: "Time executing, output as a descriptive string"
      group_label: "Time Executing Buckets"
      label: "Run Time Tier"
      type: tier
      tiers: [0,10,20,30,60,120,240]
      style: interval
      sql: ${TABLE}.total_exec_time::float/1000000 ;;
      value_format_name: decimal_0
    }

    dimension: time_executing_roundup10 {
      description: "Time executing, rounded up to the nearest 10 seconds"
      group_label: "Time Executing Buckets"
      label: "10 seconds"
      type: number
      sql: CEILING(${TABLE}.total_exec_time::float/1000000 / 10)*10 ;;
      value_format_name: decimal_0
    }

    dimension: time_executing_roundup15 {
      description: "Time executing, rounded up to the nearest 15 seconds"
      group_label: "Time Executing Buckets"
      label: "15 seconds"
      type: number
      sql: ${TABLE}.total_exec_time::float/1000000 / 15)*15 ;;
      value_format_name: decimal_0
    }

    dimension: time_overall {
      type: number
      description: "Amount of time that a query took (both queued and executing), in seconds"
      sql: ${time_in_queue} + ${time_executing}  ;;
    }

    dimension: time_elapsed {
      hidden: yes
      type: number
      description: "Amount of time (from another table, for comparison...)"
      sql: ${TABLE}.elapsed / 1000000 ;;
    }

    dimension:  was_queued {
      type: yesno
      sql: ${TABLE}.total_queue_time > 0;;
    }


    # MEASURES #

    measure: count {
      type: count
      drill_fields: [query, start_date, time_executing, pdt, looker_history_id, snippet ]
    }

    measure: count_of_queued {
      type: sum
      sql: ${was_queued}::int ;;
    }

    measure: percent_queued {
      type: number
      value_format_name: percent_1
      sql: 1.0*${count_of_queued} / NULLIF(${count}, 0)  ;;
    }

    measure: total_time_in_queue {
      type: sum
      description: "Sum of time that queries were queued before running, in seconds"
      sql: ${time_in_queue};;
      value_format_name: decimal_1
    }

    measure: total_time_executing {
      type: sum
      description: "Sum of time that queries were executing, in seconds"
      sql: ${time_executing};;
      value_format_name: decimal_1
    }

    measure: total_time_overall {
      type: sum
      description: "Sum of time that queries took (both queued and executing), in seconds"
      sql: ${time_in_queue} + ${time_executing}  ;;
      value_format_name: decimal_1
    }

    measure: avg_time_in_queue {
      type: number
      description: "Average time that queries were queued before running, in seconds"
      sql: 1.0*${total_time_in_queue}/NULLIF(SUM(${was_queued}::int), 0);;
      value_format_name: decimal_1
    }

    measure: avg_time_executing {
      type: average
      description: "Average time that queries were executing, in seconds"
      sql: ${time_executing};;
      value_format_name: decimal_1
    }

    measure: time_executing_per_query {
      hidden: yes
      type: number
      sql: CASE WHEN ${count}<>0 THEN ${total_time_executing} / ${count} ELSE NULL END ;;
      value_format_name: decimal_1
    }
  }
