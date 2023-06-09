import os

import boto3
import logging

from urllib.parse import urlparse, unquote

from utilities.config import S3Config


def extract_file_name_from_s3_url(s3_url: str) -> str:
    """
    Extracts the name of the file from s3_url.
    :param s3_url:
    :return: The name of the file.
    """
    parsed_url = urlparse(s3_url)
    path = parsed_url.path
    file_name = path.rsplit('/', 1)[-1]  # Extract the last part after the last slash
    decoded_file_name = unquote(file_name)  # Decode URL-encoded file name
    return decoded_file_name


class S3Client:
    """
    This class is responsible for storing and retrieving files from S3.
    """

    def __init__(self):
        self.s3_config = S3Config()
        self.s3 = boto3.client('s3',
                               region_name=self.s3_config.region_name,
                               aws_access_key_id=self.s3_config.aws_access_key_id,
                               aws_secret_access_key=self.s3_config.aws_secret_access_key)

    def put_file(self, src_file_name: str, target_file_name: str) -> str:
        logging.info(f'Uploading {src_file_name} to S3')
        self.s3.upload_file(src_file_name, self.s3_config.bucket_name, target_file_name)
        url = self.s3.generate_presigned_url('get_object',
                                             Params={'Bucket': self.s3_config.bucket_name,
                                                     'Key': target_file_name},
                                             ExpiresIn=self.s3_config.file_signed_url_expiration_seconds)
        return url

    def get_file(self, file_name: str) -> str:
        # Extract the file name from the URL.
        extracted_file_name = extract_file_name_from_s3_url(file_name)
        # Get the extension of the file.
        file_extension = os.path.splitext(extracted_file_name)[1]
        # Create a temp file name.
        tmp_file_name = f'temp{file_extension}'
        logging.info(f'Downloading {extracted_file_name} from S3')
        # Download the file from S3.
        extracted_file_name = extract_file_name_from_s3_url(file_name)
        self.s3.download_file(self.s3_config.bucket_name, extracted_file_name, tmp_file_name)
        return tmp_file_name
