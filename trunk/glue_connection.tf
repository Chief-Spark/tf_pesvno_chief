# =============================================================================
# Glue Connection — Red VPC para acceso a Redshift
# Permite que el Glue Job PESVNO corra dentro de la VPC del cluster Redshift.
# =============================================================================

resource "aws_glue_connection" "pesvno_vpc_redshift" {
  name        = "${var.stage}-${local.account_id}-${local.default_name_resources}-vpc-redshift"
  description = "Conexión de red VPC para que el Glue Job PESVNO acceda a Redshift"

  connection_type = "NETWORK"

  connection_properties = {
    "JDBC_CONNECTION_URL" = "jdbc:redshift://${var.redshift_host}:5440/${var.redshift_database}"
  }

  physical_connection_requirements {
    availability_zone      = var.glue_connection_availability_zone
    subnet_id              = var.glue_connection_subnet_id
    security_group_id_list = [var.glue_connection_security_group_id]
  }
}