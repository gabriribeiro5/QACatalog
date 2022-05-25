resource "aws_transfer_server" "sftp-server" {
  
  tags = merge(
    var.dlr_tags,
    {
      Description: "transfer server sftp-server - transfer-family module"
      Environment: var.env_tags[var.env]
    }
  )
}

resource "aws_transfer_user" "lideranca" {
  server_id = aws_transfer_server.sftp-server.id
  user_name = "dlr-lideranca"
  role      = var.role_transfer

  tags = merge(
    var.dlr_tags,
    {
      Description: "transfer user lideranca - transfer-family module"
      Environment: var.env_tags[var.env]
    }
  )
  home_directory = ""

  home_directory_type = "LOGICAL"
  home_directory_mappings {
      entry  = "/"
      target = "/dlr-${terraform.workspace}-bucket-transfer/partners/lideranca"
  }

}

#resource "aws_transfer_ssh_key" "lideranca_key" {
#  server_id = aws_transfer_server.sftp-server.id
#  user_name = aws_transfer_user.lideranca.user_name
#  body      = var.ssh_key_lideranca
#}
