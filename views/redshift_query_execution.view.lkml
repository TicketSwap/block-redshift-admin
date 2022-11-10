view: redshift_query_execution {

  derived_table: {
    datagroup_trigger: nightly
    distribution: "query"
    sortkeys: ["query"]
    sql:
        SELECT
          query ||'.'|| seg || '.' || step as id,
          query as query, seg, step,
          label::varchar,
          regexp_substr(label, '^[A-Za-z]+')::varchar as operation,
          CASE WHEN label ilike 'scan%name=%' AND label not ilike '%Internal Worktable'
              THEN substring(regexp_substr(label, 'name=(.+)$'),6)
              ELSE NULL
          END::varchar as "table",
          CASE WHEN label ilike 'scan%tbl=%'
              THEN ('0'+COALESCE(substring(regexp_substr(label, 'tbl=([0-9]+)'),5),''))::int
              ELSE NULL
          END as "table_id",
          CASE WHEN label ilike 'scan%tbl=%'
               THEN CASE WHEN label ilike '%name=%LR$%'
                         THEN 'name:'||substring(regexp_substr(label, 'name=(.+)$'),6)
                         ELSE 'id:'||COALESCE(substring(regexp_substr(label, 'tbl=([0-9]+)'),5),'')
                         END
               ELSE NULL
               END::varchar
          as "table_join_key",
          MAX(is_diskbased) as is_diskbased,
          MAX(is_rrscan) as is_rrscan,
          AVG(avgtime) as avgtime,
          MAX(maxtime) as maxtime,
          SUM(workmem) as workmem,
          SUM(rows_pre_filter) rows_pre_filter,
          SUM(bytes) as bytes
        FROM svl_query_summary
        WHERE query>=(SELECT min(query) FROM ${redshift_queries.SQL_TABLE_NAME})
        AND query<=(SELECT max(query) FROM ${redshift_queries.SQL_TABLE_NAME})
        GROUP BY query, seg, step, label
      ;;
  }

  # DIMENSIONS #

  dimension: step {
    type:  string
    sql: ${TABLE}.seg || '.' || ${TABLE}.step;;
    value_format_name: decimal_2
    order_by_field: step_sort
  }

  dimension: step_sort {
    hidden:  yes
    type: number
    sql: ${TABLE}.seg*10000 + ${TABLE}.step;;
  }

  dimension: query {
    type: number
    sql: ${TABLE}.query ;;
  }

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id;;
  }

  dimension: label {
    type:  string
    sql: ${TABLE}.label ;;
  }

  dimension: operation {
    type:  string
    sql: ${TABLE}.operation ;;
  }

  dimension: table {
    type: string
    sql: ${TABLE}.table ;;
  }

  dimension: table_id {
    type: number
    sql: ${TABLE}.table_id ;;
  }

  dimension: table_join_key {
    hidden: yes
    type: string
    sql: ${TABLE}.table_join_key;;
  }

  dimension: was_diskbased {
    type: string
    label: "Was disk-based?"
    description: "Whether this step of the query was executed as a disk-based operation on any slice in the cluster"
    sql: CASE WHEN ${TABLE}.is_diskbased='t' THEN 'Yes' ELSE 'No' END;;
    html:
          {% if value == 'Yes' %}
            <span style="color: darkred">{{ rendered_value }}</span>
          {% else %}
            {{ rendered_value }}
          {% endif %}
    ;;
  }

  dimension: was_restricted_scan {
    type: yesno
    label: "Was the scan range-restricted?"
    description: "Whether this step of the query was executed as a disk-based operation on any slice in the cluster"
    sql: CASE WHEN ${TABLE}.is_rrscan='t' THEN 'Yes' WHEN ${operation} = 'scan' THEN 'No' ELSE 'N/A' END;;
    html:
          {% if value == 'Yes' %}
            <span style="color: green">{{ rendered_value }}</span>
          {% else %}
            {{ rendered_value }}
          {% endif %}
    ;;
  }

  dimension: step_average_slice_time {
    type: number
    description: "Average time among slices, in seconds, for this step"
    sql: ${TABLE}.avgtime/1000000 ;;
  }

  dimension: step_max_slice_time {
    type: number
    description: "Maximum time among slices, in seconds, for this step"
    sql: ${TABLE}.maxtime/1000000 ;;
  }

  dimension: step_skew {
    type: number
    description: "The ratio of max execution time vs avg execution time for this step among participating slices. (For information on how many slices participated in this step, check svl_query_report)"
    sql: CASE WHEN ${TABLE}.avgtime=0 THEN NULL ELSE ${TABLE}.maxtime / ${TABLE}.avgtime END ;;
    html:
          {% if value > 16 %}
            <span style="color: darkred">{{ rendered_value }}</span>
          {% elsif value >4 %}
            <span style="color: darkorange">{{ rendered_value }}</span>
          {% else %}
            {{ rendered_value }}
          {% endif %}
    ;;
  }

  dimension: working_memory {
    type: number
    description: "Amount of working memory (in bytes) assigned to the query step"
    sql: ${TABLE}.workmem ;;
  }

  dimension: rows_out {
    type: number
    description: "For scans of permanent tables, the total number of rows emitted (before filtering rows marked for deletion, a.k.a ghost rows). If very different from Query Plan rows, stats should be updated"
    sql: ${TABLE}.rows_pre_filter ;;
  }

  dimension: bytes {
    type: number
    sql:  ${TABLE}.bytes ;;
  }

  # MEASURES #

  measure:  count {
    hidden: yes
    type: count
  }

  measure: any_disk_based {
    type: string
    sql: MAX(${was_diskbased}) ;;
    html:
      {% if value == 'Yes' %}
      <span style="color: darkred; font-weight:bold">{{ rendered_value }}</span>
      {% elsif value == 'No' %}
      <span style="color: green">{{ rendered_value }}</span>
      {% else %}
      {{ rendered_value }}
      {% endif %}
    ;;
  }

  measure: any_restricted_scan {
    type: string
    sql: MAX(${was_restricted_scan}) ;;
    html:
      {% if value == 'Yes' %}
      <span style="color: green">{{ rendered_value }}</span>
      {% elsif value == 'No' %}
      <span style="color: darkorange">{{ rendered_value }}</span>
      {% else %}
      {{ rendered_value }}
      {% endif %}
    ;;
  }

  measure:  _count_restricted_scan {
    hidden: yes
    type:  sum
    sql: CASE WHEN ${operation}='scan' AND ${table} IS NOT NULL AND ${TABLE}.is_rrscan='t' THEN 1 ELSE 0 END ;;
  }

  measure: count_scans {
    type:sum
    sql: CASE WHEN ${operation}='scan' AND ${table} IS NOT NULL THEN 1 ELSE 0 END ;;
  }

  measure: percent_restricted_scan {
    type: number
    sql: CASE WHEN ${count_scans} = 0 THEN NULL
      ELSE ${_count_restricted_scan} / ${count_scans} END ;;
    html:
      {% if value <= 0.10 %}
        <span style="color: darkred">{{ rendered_value }}</span>
      {% elsif value <= 0.50 %}
        <span style="color: darkorange">{{ rendered_value }}</span>
      {% elsif value >= 0.90 %}
        <span style="color: green">{{ rendered_value }}</span>
      {% else %}
        {{ rendered_value }}
      {% endif %}
    ;;
    value_format_name: percent_1
  }

  measure: emitted_rows_to_table_rows_ratio {
    type: number
    sql: CASE WHEN SUM(${redshift_tables.rows_in_table}) = 0 OR ${count} = 0 THEN NULL
      ELSE ${total_rows_emitted} / (${redshift_tables.total_rows} * ${count}) END ;;
    # Using hard-coded SUM to avoid unneccessary symmetric aggregate just to check SUM <> 0
    value_format_name: percent_1
  }

  measure: total_bytes_distributed {
    type: sum
    sql: CASE WHEN ${operation} = 'dist' THEN ${bytes} ELSE 0 END ;;
  }

  measure: total_bytes_broadcast {
    type: sum
    sql: CASE WHEN ${operation} = 'bcast' THEN ${bytes} ELSE 0 END ;;
  }

  measure: total_bytes_scanned {
    type: sum
    sql: CASE WHEN ${TABLE}.operation = 'scan' THEN ${bytes} ELSE 0 END ;;
  }

  measure: total_rows_emitted {
    type: sum
    sql: CASE WHEN ${operation} = 'scan' THEN ${rows_out} ELSE 0 END ;;
  }

  measure: total_O_rows_sorted {
    hidden: yes
    type: sum
    sql: CASE
        WHEN  ${operation} = 'sort' THEN
          CASE WHEN ${rows_out}<=1 THEN 1 ELSE ${rows_out} * LN(${rows_out}) / LN(2) END
        ELSE 0
      END ;;
  }

  measure: total_rows_sorted_approx {
    type: number
    description: "Aggregates multiple n log(n) time-complexity sortings by comparing them to one sort that would have approximately the same time complexity"
    #http://cs.stackexchange.com/questions/44944/n-log-n-c-what-are-some-good-approximations-of-this
    #1st answer with an added first order Newton approximation
    sql:CASE WHEN ${total_O_rows_sorted}<2 THEN ${total_O_rows_sorted}
          ELSE LN(2)*${total_O_rows_sorted}*(1+LN(ln((${total_O_rows_sorted}/LN(${total_O_rows_sorted})*LN(2)))/LN(2))/LN(2)/(LN((${total_O_rows_sorted}/LN(${total_O_rows_sorted})*LN(2)))/LN(2)))/LN(${total_O_rows_sorted})
          END;;
    value_format_name: decimal_0
  }

  measure: max_step_skew {
    type: max
    sql: ${step_skew} ;;
  }

  measure: average_step_skew {
    type: average
    sql: ${step_skew} ;;
  }
}
