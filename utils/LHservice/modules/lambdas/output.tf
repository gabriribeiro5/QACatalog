output "lambda_discador_arn" {
  value = aws_lambda_function.lambda_start_validator_dialer.arn
}

output "lambda_sms_arn" {
  value = aws_lambda_function.lambda_start_validator_message.arn
}

output "lambda_acordo_arn" {
  value = aws_lambda_function.lambda_start_validator_acordo.arn
}

output "lambda_apply_changes_arn" {
  value = aws_lambda_function.lambda_start_apply_changes_acordo.arn
}