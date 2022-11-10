view: redshift_slices {
  # http://docs.aws.amazon.com/redshift/latest/dg/r_STV_SLICES.html
  # Use the STV_SLICES table to view the current mapping of a slice to a node.
  # This table is visible to all users. Superusers can see all rows; regular users can see only their own data.
  derived_table: {
    datagroup_trigger: nightly
    distribution_style: "all"
    sortkeys: ["node"]
    sql: SELECT slice,node FROM STV_SLICES;;
  }

  # DIMENSIONS #

  dimension: node {
    type: number
    value_format_name: id
    sql: ${TABLE}.node ;;
    description: "Cluster node where the slice is located"
  }

  dimension: slice {
    type: number
    value_format_name: id
    sql: ${TABLE}.slice ;;
    primary_key: yes
    description: "Node slice"
  }

  # MEASURES #

  measure: nodes {
    type: count_distinct
    sql: ${node} ;;
  }

  measure:  slices {
    type: count_distinct
    sql: ${slice} ;;
  }
}
