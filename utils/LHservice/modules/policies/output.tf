output "sagemaker_role" {
  value = aws_iam_role.role_sagemaker.arn
}

output "glue_role" {
  value = aws_iam_role.role_glue.arn
}

output "glue_role_redshift" {
  value = aws_iam_role.role_glue_redshift.arn
}

output "glue_role_id" {
  value = aws_iam_role.role_glue.id
}

output "security_group" {
  value = aws_security_group.glue-security-group
}

output "security_group_id" {
  value = aws_security_group.glue-security-group.id
}

output "redshift_role" {
  value = aws_iam_role.role_redshift.arn
}

output "lambda_redshift_role" {
  value = aws_iam_role.role_lambda_redshift.arn
}

output "lambda_stepfunctions_role" {
  value = aws_iam_role.role_lambda_stepfunctions.arn
}

output "stepfunctions_lambda_role" {
  value = aws_iam_role.role_stepfunctions_lambda.arn
}

output "transfer_role" {
  value = aws_iam_role.role_transfer.arn
}

output "lambda_bucket_role" {
  value = aws_iam_role.role_lambda_s3.arn
}