view: redshift_data_loads {
  derived_table: {
    sql: select replace(regexp_substr(filename, '//[a-zA-Z0-9\-]+/'), '/', '') as root_bucket
        , replace(filename, split_part(filename, '/', regexp_count(filename, '/') + 1), '') as s3_path
        , regexp_replace(replace(filename, split_part(filename, '/', regexp_count(filename, '/') + 1), ''), '([\\d]{5,}|[\\d\-]{5,}/)', '') as s3_path_clean
        , split_part(filename, '/', regexp_count(filename, '/') + 1) as file_name
        , regexp_replace(split_part(filename, '/', regexp_count(filename, '/') + 1), '([\\d]{7,}|[\\d\-]{7,})', '') as file_stem
        , curtime as load_time
      from stl_load_commits
       ;;
  }

  # DIMENSIONS #

  dimension: root_bucket {
    type: string
    sql: ${TABLE}.root_bucket ;;
  }

  dimension: s3_path {
    type: string
    sql: ${TABLE}.s3_path ;;
  }

  dimension: s3_path_clean {
    type: string
    sql: ${TABLE}.s3_path_clean ;;
  }

  dimension: file_name {
    type: string
    sql: ${TABLE}.file_name ;;
  }

  dimension: file_stem {
    type: string
    sql: ${TABLE}.file_stem ;;
  }

  dimension_group: load {
    type: time
    timeframes: [raw, time, date]
    sql: ${TABLE}.load_time ;;
    description: "Time that this entry was last updated"
  }

  # MEASURES #

  measure: most_recent_load {
    type: string
    sql: max(${load_raw}) ;;
  }

  measure: hours_since_last_load {
    type: number
    value_format_name: id
    sql: datediff('hour', ${most_recent_load}, getdate()) ;;
    html: {% if value < 24 %}
      <div style="color:#B40404; background-color:#22CE7E; font-size:100%; text-align:center">{{ rendered_value }}</div>
      {% elsif value >= 24 and value < 48 %}
      <div style="color:#868A08; background-color:#95F047; font-size:100%; text-align:center">{{ rendered_value }}</div>
      {% elsif value >= 48 %}
      <div style="color:#868A08; background-color:#C64646; font-size:100%; text-align:center">{{ rendered_value }}</div>
      {% endif %}
      ;;
  }
}
