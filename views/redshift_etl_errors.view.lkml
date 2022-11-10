view: redshift_etl_errors {
  derived_table: {
    sql: select starttime as error_time
        , filename as file_name
        , colname as column_name
        , type as column_data_type
        , position as error_position
        , raw_field_value as error_field_value
        , err_reason as error_reason
        , raw_line
      from stl_load_errors
       ;;
  }

  # DIMENSIONS #

  dimension_group: error {
    type: time
    timeframes: [time, date]
    sql: ${TABLE}.error_time ;;
    description: "Start time in UTC for the load"
  }

  dimension: file_name {
    type: string
    sql: ${TABLE}.file_name ;;
    description: "Complete path to the input file for the load"
  }

  dimension: column_name {
    type: string
    sql: ${TABLE}.column_name ;;
    description: "Field with the error"
  }

  dimension: column_data_type {
    type: string
    sql: ${TABLE}.column_data_type ;;
    description: "Data Type of the field"
  }

  dimension: error_position {
    type: string
    sql: ${TABLE}.error_position ;;
    description: "Position of the error in the field"
  }

  dimension: error_field_value {
    type: string
    sql: ${TABLE}.error_field_value ;;
    description: "The pre-parsing value for the field 'colname' that lead to the parsing error"
  }

  dimension: error_reason {
    type: string
    sql: ${TABLE}.error_reason ;;
    description: "Explanation for the error"
  }

  dimension: raw_line {
    type: string
    sql: ${TABLE}.raw_line ;;
    description: "Raw load data that contains the error. Multibyte characters in the load data are replaced with a period"
  }
}
