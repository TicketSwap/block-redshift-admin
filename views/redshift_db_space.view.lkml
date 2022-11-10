view: redshift_db_space {
  derived_table: {
    sql: select name as table
        , trim(pgn.nspname) as schema
        , sum(b.mbytes) as megabytes
        , sum(a.rows) as rows
      from (select db_id
              , id
              , name
              , sum(rows) as rows
            from stv_tbl_perm a
            group by 1,2,3) as a
      join pg_class as pgc
      on pgc.oid = a.id
      join pg_namespace as pgn
      on pgn.oid = pgc.relnamespace
      join pg_database as pgdb
      on pgdb.oid = a.db_id
      join (select tbl
              , count(*) as mbytes
            from stv_blocklist
            group by 1) as b
      on a.id = b.tbl
      group by 1,2
       ;;
  }

  # DIMENSIONS #

  dimension: pk {
    sql: ${table} || ${schema} ;;
    primary_key: yes
  }

  dimension: table {
    type: string
    sql: ${TABLE}.table ;;
  }

  dimension: schema {
    type: string
    sql: ${TABLE}.schema ;;
  }

  dimension: megabytes {
    type: number
    sql: ${TABLE}.megabytes ;;
  }

  dimension: rows {
    type: number
    sql: ${TABLE}.rows ;;
  }

  dimension: table_stem {
    sql: case
        when (${table} ~ '(lr|lc)\\$[a-zA-Z0-9]+_.*')
        then ltrim(regexp_substr(${table}, '_.*'), '_') || ' - Looker PDT'
        else ${table}
      end
       ;;
  }

  # MEASURES #

  measure: total_megabytes {
    type: sum
    sql: ${megabytes} ;;
  }

  measure: total_rows {
    type: sum
    sql: ${rows} ;;
  }

  measure: total_tables {
    type: count_distinct
    sql: ${table} ;;
  }
}
