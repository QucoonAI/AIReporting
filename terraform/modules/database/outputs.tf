output "rds_endpoint" {
  description = "RDS instance endpoint"
  value       = aws_db_instance.postgres.endpoint
}

output "rds_port" {
  description = "RDS instance port"
  value       = aws_db_instance.postgres.port
}

output "rds_database_name" {
  description = "RDS database name"
  value       = aws_db_instance.postgres.db_name
}

output "rds_username" {
  description = "RDS master username"
  value       = aws_db_instance.postgres.username
  sensitive   = true
}

output "rds_identifier" {
  description = "RDS instance identifier"
  value       = aws_db_instance.postgres.identifier
}

output "rds_arn" {
  description = "RDS instance ARN"
  value       = aws_db_instance.postgres.arn
}

output "db_subnet_group_name" {
  description = "Database subnet group name"
  value       = aws_db_subnet_group.main.name
}

output "connection_string" {
  description = "Database connection string"
  value       = "postgresql://${aws_db_instance.postgres.username}:${var.db_password}@${aws_db_instance.postgres.endpoint}:${aws_db_instance.postgres.port}/${aws_db_instance.postgres.db_name}"
  sensitive   = true
}