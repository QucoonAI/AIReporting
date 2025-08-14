resource "aws_db_subnet_group" "main" {
  name       = "${var.project_name}-${var.environment}-db-subnet-group"
  subnet_ids = [var.private_subnet_id, var.public_subnet_id]

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-db-subnet-group"
    Type = "Database"
  })
}

# RDS PostgreSQL Instance
resource "aws_db_instance" "postgres" {
  identifier = "${var.project_name}-${var.environment}-postgres"

  # Engine Configuration
  engine         = "postgres"
  engine_version = var.postgres_version
  instance_class = var.db_instance_class

  # Storage Configuration
  allocated_storage     = var.allocated_storage
  max_allocated_storage = var.max_allocated_storage
  storage_type          = var.storage_type
  storage_encrypted     = var.storage_encrypted

  # Database Configuration
  db_name  = "${replace(var.project_name, "-", "_")}_${var.environment}"
  username = var.db_username
  password = var.db_password

  # Network & Security
  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [var.lambda_security_group_id]
  publicly_accessible    = var.publicly_accessible

  # Backup Configuration
  backup_retention_period = var.backup_retention_period
  backup_window          = var.backup_window
  maintenance_window     = var.maintenance_window

  # Deletion & Final Snapshot
  skip_final_snapshot = var.environment != "prod"
  deletion_protection = var.environment == "prod"
  final_snapshot_identifier = var.environment == "prod" ? "${var.project_name}-${var.environment}-final-snapshot-${formatdate("YYYY-MM-DD-hhmm", timestamp())}" : null

  # Performance Insights (optional)
  performance_insights_enabled = var.performance_insights_enabled
  performance_insights_retention_period = var.performance_insights_enabled ? var.performance_insights_retention_period : null

  # Monitoring
  monitoring_interval = var.monitoring_interval
  monitoring_role_arn = var.monitoring_interval > 0 ? aws_iam_role.rds_enhanced_monitoring[0].arn : null

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-postgres"
    Type = "Database"
  })

  depends_on = [aws_db_subnet_group.main]
}

# Enhanced Monitoring IAM Role (only if monitoring is enabled)
resource "aws_iam_role" "rds_enhanced_monitoring" {
  count = var.monitoring_interval > 0 ? 1 : 0
  
  name = "${var.project_name}-${var.environment}-rds-enhanced-monitoring"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "monitoring.rds.amazonaws.com"
        }
      }
    ]
  })

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-rds-monitoring-role"
  })
}

resource "aws_iam_role_policy_attachment" "rds_enhanced_monitoring" {
  count = var.monitoring_interval > 0 ? 1 : 0
  
  role       = aws_iam_role.rds_enhanced_monitoring[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonRDSEnhancedMonitoringRole"
}