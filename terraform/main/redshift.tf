resource "aws_redshiftserverless_namespace" "main" {
  namespace_name      = "${var.project_name}-${var.environment}"
  db_name             = var.redshift_db_name
  admin_username      = var.redshift_admin_username
  admin_user_password = random_password.redshift.result
  iam_roles           = [aws_iam_role.redshift_s3.arn]
  log_exports         = ["userlog", "connectionlog", "useractivitylog"]
}

resource "aws_redshiftserverless_workgroup" "main" {
  namespace_name      = aws_redshiftserverless_namespace.main.namespace_name
  workgroup_name      = "${var.project_name}-${var.environment}-wg"
  base_capacity       = 8
  subnet_ids          = [aws_subnet.public_a.id, aws_subnet.public_b.id]
  security_group_ids  = [aws_security_group.redshift.id]
  publicly_accessible = true

  depends_on = [
    aws_redshiftserverless_namespace.main,
    aws_internet_gateway.main
  ]
}
