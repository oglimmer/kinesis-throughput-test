
##
## GLOBAL
##


terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  region  = "eu-central-1"
  profile = "sy"
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

##
## KINESIS
##

resource "aws_kinesis_stream" "inbound_stream" {
  name             = "inbound"
  shard_count      = 4

  shard_level_metrics = [
    "OutgoingBytes",
    "IteratorAgeMilliseconds",
    "OutgoingRecords",
    "IncomingBytes",
    "IncomingRecords",
    "ReadProvisionedThroughputExceeded",
    "OutgoingRecords",
    "WriteProvisionedThroughputExceeded"
  ]

}

resource "aws_kinesis_stream" "outbound_stream" {
  name             = "outbound"
  shard_count      = 1

  shard_level_metrics = [
    "OutgoingBytes",
    "IteratorAgeMilliseconds",
    "OutgoingRecords",
    "IncomingBytes",
    "IncomingRecords",
    "ReadProvisionedThroughputExceeded",
    "OutgoingRecords",
    "WriteProvisionedThroughputExceeded"
  ]

}

