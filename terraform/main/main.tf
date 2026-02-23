terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws     = { source = "hashicorp/aws",       version = "~> 5.0" }
    random  = { source = "hashicorp/random",    version = "~> 3.0" }
    archive = { source = "hashicorp/archive",   version = "~> 2.0" }
    tls     = { source = "hashicorp/tls",       version = "~> 4.0" }
    local   = { source = "hashicorp/local",     version = "~> 2.0" }
  }

  backend "s3" {
    bucket         = "data-platform-tf-state-c641a84a"
    key            = "data-platform/dev/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "data-platform-tf-lock"
    encrypt        = true
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}
