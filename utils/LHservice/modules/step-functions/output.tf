output "state_machine_discador_arn" {
  value = aws_sfn_state_machine.sfn_state_machine_lideranca_discador.arn
}

output "state_machine_sms_arn" {
  value = aws_sfn_state_machine.sfn_state_machine_lideranca_sms.arn
}

output "state_machine_validator_dialer_arn" {
  value = aws_sfn_state_machine.sfn_state_machine_validator_dialer.arn
}

output "state_machine_validator_message_arn" {
  value = aws_sfn_state_machine.sfn_state_machine_validator_message.arn
}

output "state_machine_apply_changes_discador_arn" {
  value = aws_sfn_state_machine.sfn_state_machine_apply_changes_parceiros_discador.arn
}

output "state_machine_validator_acordo_arn" {
  value = aws_sfn_state_machine.sfn_state_machine_validator_coligadas_acordo.arn
}

output "state_machine_apply_changes_acordo_arn" {
  value = aws_sfn_state_machine.sfn_state_machine_apply_changes_coligadas_acordo.arn
}

