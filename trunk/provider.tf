terraform {
  required_version = ">= 1.7.2"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.67"
    }
  }
}

provider "aws" {
  region = var.region
  assume_role {
    role_arn     = var.assume_role_arn
    external_id  = var.assume_role_external_id
    session_name = "terraform"
  }

  default_tags {
    tags = var.required_tags
  }
}