import boto3


def check_s3_folder_exists(bucket_name, out_path):
    """
        S3에 저장된 Parquet 데이터 경로가 존재하는지 확인하는 함수.

        이 함수는 지정된 S3 버킷과 경로(prefix)를 기준으로 기존 Parquet 데이터가 존재하는지 확인합니다.
        주로 기존 데이터가 있는 경우 중복 제거(deduplication) 작업 또는 데이터 덮어쓰기를 결정하는 데 사용됩니다.
    """
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=out_path, Delimiter='/')
    return 'Contents' in response or 'CommonPrefixes' in response
