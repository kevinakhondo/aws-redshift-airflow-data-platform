# 1. variables.tf
##Specify the region
variable "aws_region" {
  type    = string
  default = "us-east-1"
}
##Specify the name of the proct
variable "project_name" {
  type    = string
  default = "data-platform"
}
## Specify the working environment
variable "environment" {
  type    = string
  default = "dev"
}
##Specify the IP address(Your machines) for public accessibility
variable "your_ip" {
  description = "This is the IP of my local machine"
  type        = string
}
#Specify the redshift admin name
variable "redshift_admin_username" {
  type    = string
  default = "dataplatform_admin"
}
##Specify the redshift db name
variable "redshift_db_name" {
  type    = string
  default = "dataplatform"
}
