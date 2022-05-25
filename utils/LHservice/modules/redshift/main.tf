resource "aws_redshift_cluster" "redshift-cluster" {
  cluster_identifier        = "dlr-redshift-cluster"
  database_name             = var.redshift_db
  master_username           = var.redshift_user
  master_password           = var.redshift_pass
  enhanced_vpc_routing      = true
  node_type                 = var.node_type
  cluster_type              = var.redshift_cluster_type[var.env]
  number_of_nodes           = var.redshift_nodes[var.env]
  skip_final_snapshot       = true
  encrypted                 = true
  availability_zone         = var.availability_zone[var.env]
  cluster_subnet_group_name = var.redshift_subnet

  tags = merge(
    var.dlr_tags,
    {
      Description: "Redshift cluster - Redshift module"
      Environment: var.env_tags[var.env]
    }
  )
}