view: redshift_tables {
  derived_table: {
    datagroup_trigger: nightly
    distribution_style: all
    indexes: ["table_id","table"]
    # http://docs.aws.amazon.com/redshift/latest/dg/r_SVV_TABLE_INFO.html
    sql: select
        "database"::varchar,
        "schema"::varchar,
        "Table_id"::bigint,
        "table"::varchar,
        "encoded"::varchar,
        "diststyle"::varchar,
        REGEXP_REPLACE("sortkey1", '[^ -~]', '?')::varchar as "sortkey1",
        "max_varchar"::bigint,
        "sortkey1_enc"::varchar,
        "sortkey_num"::int,
        "size"::bigint,
        "pct_used"::numeric,
        "unsorted"::numeric,
        "stats_off"::numeric,
        "tbl_rows"::bigint,
        "skew_sortkey1"::numeric,
        "skew_rows"::numeric
      from svv_table_info
    ;;
  }

  # DIMENSIONS #

  # Identifiers {
  dimension: table_id {
    group_label: " Identifiers"
    description: "The table's internal/numeric ID"
    type: number
    sql: ${TABLE}.table_id ;;
  }

  dimension: path {
    group_label: " Identifiers"
    description: "Database + schema + table name"
    sql: ${database}||'.'||${schema}||'.'||${table} ;;
    primary_key: yes
  }

  dimension: database {
    group_label: " Identifiers"
    type: string
    sql: ${TABLE}.database ;;
    description: "Database name"
  }

  dimension: schema {
    group_label: " Identifiers"
    type: string
    sql: ${TABLE}.schema ;;
    description: "Schema name"
  }

  dimension: table {
    group_label: " Identifiers"
    label: "Table Name"
    description: "Local table name"
    type: string
    sql: ${TABLE}."table" ;;
  }

  dimension: table_join_key {
    group_label: " Identifiers"
    hidden:yes
    type:string
    sql: CASE WHEN ${schema}='looker_scratch'
              THEN 'name:'||${table}
              ELSE 'id:'||${table_id}
           END ;;
    #Because when PDTs get rebuilt, their ID changes, and showing the info about the current PDT is more useful than showing nothing
  }
  #}

  # Size (Table) {
  dimension: rows_in_table {
    group_label: "Size (Table)"
    type: number
    sql: ${TABLE}.tbl_rows ;;
    description: "Total number of rows in the table. This value includes rows marked for deletion, but not yet vacuumed"
  }

  dimension: size {
    group_label: "Size (Table)"
    description: "Size of the table, in 1 MB data blocks"
    type: number
    sql: ${TABLE}.size ;;
  }

  dimension: pct_used {
    group_label: "Size (Table)"
    label: "Percentage used"
    type: number
    description: "Percent of available space that is used by the table"
    sql: ${TABLE}.pct_used ;;
  }
  #}

  # Size (Columns) {
  dimension: encoded {
    group_label: "Size (Columns)"
    description: "Whether any column has compression encoding defined"
    type: yesno
    sql: case ${TABLE}.encoded
        when 'Y'
        then true
        when 'N'
        then false
        else null
      end
       ;;
  }

  dimension: max_varchar {
    group_label: "Size (Columns)"
    description: "Size of the largest column that uses a VARCHAR data type"
    type: number
    sql: ${TABLE}.max_varchar ;;
  }
  #}

  # Distribution {
  dimension: distribution_style {
    group_label: "Distribution"
    type: string
    sql: ${TABLE}.diststyle ;;
    html:
        {% if value == 'EVEN' %}
          <span style="color: darkorange">{{ rendered_value }}</span>
        {% elsif value == 'ALL' or value == 'DS_DIST_NONE'%}
          <span style="color: dimgray">{{ rendered_value }}</span>
        {% else %}
          {{ rendered_value }}
        {% endif %}
        ;;
    description: "Distribution style or distribution key column, if key distribution is defined"
  }
  dimension: skew_rows {
    group_label: "Distribution"
    description: "Ratio of the number of rows in the slice with the most rows to the number of rows in the slice with the fewest rows"
    type: number
    sql: ${TABLE}.skew_rows ;;
    html:
            {% if value >= 75 %}
              <span style="color:darkred">{{ rendered_value }}</span>
            {% elsif value >= 25 %}
              <span style="color:darkorange">{{ rendered_value }}</span>
            {% else %}
              {{ rendered_value }}
            {% endif %}
      ;;
  }
  #}

  # Sorting {
  dimension: sortkey {
    group_label: "Sorting"
    description: "First column in the sort key, if a sort key is defined"
    type: string
    sql: ${TABLE}.sortkey1 ;;
  }

  dimension: sortkey_encoding {
    group_label: "Sorting"
    description: "Compression encoding of the first column in the sort key, if a sort key is defined"
    type: string
    sql: ${TABLE}.sortkey1_enc ;;
  }

  dimension: number_of_sortkeys {
    group_label: "Sorting"
    type: number
    sql: ${TABLE}.sortkey_num ;;
    description: "Number of columns defined as sort keys"
  }

  dimension: unsorted {
    group_label: "Sorting"
    description: "Percent of unsorted rows in the table"
    type: number
    sql: ${TABLE}.unsorted ;;
    html:
      {% if value >= 50 %}
        <span style="color: darkred">{{ rendered_value }}</span>
      {% elsif value >= 10 %}
        <span style="color: darkorange">{{ rendered_value }}</span>
      {% elsif value < 10 %}
        <span style="color: green">{{ rendered_value }}</span>
      {% else %}
        {{ rendered_value }}
      {% endif %}
    ;;
  }

  dimension: skew_sortkey {
    group_label: "Sorting"
    description: "Ratio of the size of the largest non-sort key column to the size of the first column of the sort key, if a sort key is defined. Use this value to evaluate the effectiveness of the sort key"
    type: number
    sql: ${TABLE}.skew_sortkey1 ;;
  }
  #}

  dimension: stats_off {
    description: "Number that indicates how stale the table's statistics are; 0 is current, 100 is out of date"
    type: number
    sql: ${TABLE}.stats_off ;;
    html:
    {% if value >= 50 %}
      <span style="color: darkred">{{ rendered_value }}</span>
    {% elsif value >= 10 %}
      <span style="color: darkorange">{{ rendered_value }}</span>
    {% elsif value < 10 %}
      <span style="color: green">{{ rendered_value }}</span>
    {% else %}
      {{ rendered_value }}
    {% endif %}
  ;;
  }

  # MEASURES #

  measure: count {
    type: count
  }

  measure: total_rows {
    type: sum
    sql: ${rows_in_table};;
  }

  measure: total_size {
    description: "Size of the table(s), in 1 MB data blocks"
    type: sum
    sql: ${size} ;;
  }
}
