import os


class S3Config:
    """
    This class stores the configurations for S3.
    """
    def __init__(self):
        self.bucket_name = 'decoverlaws'
        self.region_name = os.environ.get('S3_DEFAULT_REGION')
        self.aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
        self.file_signed_url_expiration_seconds = 3600