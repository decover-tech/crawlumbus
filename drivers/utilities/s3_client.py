import os
from typing import Tuple

import boto3
import logging

from urllib.parse import urlparse, unquote

from drivers.utilities.config import S3Config


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


def extract_bucket_and_key_from_s3_url(s3_url: str) -> Tuple[str, str]:
    parsed_url = urlparse(s3_url)
    bucket_name = parsed_url.netloc
    file_key = parsed_url.path.lstrip('/')  # remove leading '/'
    return bucket_name, file_key


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

    def upload_file(self, src_file_path: str, target_file_path: str):
        logging.info(f'Uploading {src_file_path} to {target_file_path}')
        bucket_name, file_key = extract_bucket_and_key_from_s3_url(target_file_path)

        # Extract the directory path from the file key
        directory_path = os.path.dirname(file_key)

        # Create S3 resource
        s3_resource = boto3.resource('s3',
                                     region_name=self.s3_config.region_name,
                                     aws_access_key_id=self.s3_config.aws_access_key_id,
                                     aws_secret_access_key=self.s3_config.aws_secret_access_key)

        # Create S3 bucket object
        bucket = s3_resource.Bucket(bucket_name)

        # Create directories in S3 recursively
        for path_part in directory_path.split('/'):
            if path_part:
                prefix = file_key[:file_key.index(path_part) + len(path_part)]
                bucket.put_object(Key=prefix + '/')

        # Upload the file to S3
        bucket.upload_file(src_file_path, file_key)

    def put_file(self, src_file_name: str, target_file_name: str) -> str:
        logging.info(f'Uploading {src_file_name} to {target_file_name}')
        self.s3.upload_file(src_file_name, self.s3_config.bucket_name, target_file_name)
        url = self.s3.generate_presigned_url('get_object',
                                             Params={'Bucket': self.s3_config.bucket_name,
                                                     'Key': target_file_name},
                                             ExpiresIn=self.s3_config.file_signed_url_expiration_seconds)
        return url

    def get_file(self, file_name: str) -> str:
        """
        Downloads the file from S3 and returns the name of the downloaded file.
        :param file_name: The name of the file to download.
        :return: A temporary file name where the file is downloaded.
        """
        logging.info(f'Downloading {file_name} from S3')

        if file_name.startswith('s3://'):
            bucket, file_key = extract_bucket_and_key_from_s3_url(file_name)
        else:
            bucket = self.s3_config.bucket_name
            file_key = file_name

        file_extension = os.path.splitext(file_key)[1]

        # Create a temp file name.
        tmp_file_name = f'temp{file_extension}'
        logging.info(f'Downloading {file_key} from S3 bucket {bucket}')
        self.s3.download_file(bucket, file_key, tmp_file_name)
        return tmp_file_name

    def list_files(self, s3_path: str) -> list:
        """
        List all files in the given S3 path.
        :param s3_path: The path of the directory on S3.
        :return: The list of file names.
        """
        parsed_url = urlparse(s3_path)
        bucket_name = parsed_url.netloc
        prefix = parsed_url.path.lstrip('/')
        result = self.s3.list_objects(Bucket=bucket_name, Prefix=prefix)

        # Check if the prefix is a directory.
        # If it is, remove the directory name from the file names.
        files = []
        if result.get('Contents'):
            for file in result.get('Contents'):
                file_name = file.get('Key')
                if file_name != prefix:  # Exclude the directory name
                    # Remove the directory name from the file name
                    file_name = file_name.replace(prefix, '', 1).lstrip('/')
                    files.append(file_name)

        return files

    def exists(self, s3_location: str) -> bool:
        try:
            s3 = boto3.resource('s3')
            # Extract S3 bucket and key from file_location
            bucket_name, key = extract_bucket_and_key_from_s3_url(s3_location)
            # Check if the bucket exists.
            if not s3.Bucket(bucket_name) in s3.buckets.all():
                logging.warning(f"Bucket {bucket_name} does not exist.")
                return False
            bucket = s3.Bucket(bucket_name)
            objs = list(bucket.objects.filter(Prefix=key))
            return len(objs) > 0 and objs[0].key == key
        except Exception as e:
            logging.error(f"Error while checking if file exists: {e}")
            return False
