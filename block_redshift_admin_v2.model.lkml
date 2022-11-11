connection: "redshift_prd"

include: "/views/*.view.lkml"
include: "/explores/*.explore.lkml"
include: "/dashboards/*.dashboard.lookml"

datagroup: nightly {
  sql_trigger: SELECT GETDATE()::DATE;;
}
