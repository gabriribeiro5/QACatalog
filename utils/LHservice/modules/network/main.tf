
resource "aws_redshift_subnet_group" "subnet" {
  name       = "redshift-subnet"
  subnet_ids = [var.subnet_ids_redshift[var.env]]

  tags = merge(
    var.redshift_subnet_tags,
    var.dlr_tags,
    {
      Description: "Redshift subnet group - network module"
      Environment: var.env_tags[var.env]
    }
  )
}