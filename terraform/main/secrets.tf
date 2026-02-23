resource "random_password" "redshift" {
  length           = 32
  special          = true
  override_special = "!#$%&*()-_=+[]{}<>:?"
}

resource "aws_secretsmanager_secret" "redshift_password" {
  name                    = "${var.project_name}/${var.environment}/redshift-admin-v2"
  recovery_window_in_days = 0
  # recovery_window_in_days = 0 means immediate deletion when destroyed
  # This prevents the conflict we just hit
}

resource "aws_secretsmanager_secret_version" "redshift_password" {
  secret_id = aws_secretsmanager_secret.redshift_password.id
  secret_string = jsonencode({
    username = var.redshift_admin_username
    password = random_password.redshift.result
    dbname   = var.redshift_db_name
  })
}
