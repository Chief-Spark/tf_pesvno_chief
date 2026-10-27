data "aws_s3_bucket" "s3_access_logs" {
  bucket = local.s3_access_logs_bucket_name
}

resource "aws_s3_bucket_logging" "pesvno_bucket" {
  bucket        = aws_s3_bucket.pesvno_bucket.id
  target_bucket = data.aws_s3_bucket.s3_access_logs.id
  target_prefix = "s3-access-logs/${aws_s3_bucket.pesvno_bucket.bucket}/"

  target_object_key_format {
    partitioned_prefix {
      partition_date_source = "EventTime"
    }
  }

  depends_on = [
    aws_s3_bucket.pesvno_bucket,
    data.aws_s3_bucket.s3_access_logs,
  ]
}
