import boto3
from botocore.exceptions import NoCredentialsError
import json
from datetime import datetime, timedelta

def read_file_from_s3(bucket_name: str, file_key: str) -> str:
    """
    S3에서 JSON 파일을 읽어옵니다.

    Args:
        bucket_name (str): S3 버킷 이름
        file_key (str): S3 파일 경로(Key)

    Returns:
        str: S3 파일의 내용
    """
    try:
        s3 = boto3.client('s3')  # AWS S3 클라이언트 생성
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        return obj['Body'].read().decode('utf-8')
    except NoCredentialsError:
        print("AWS 자격 증명을 찾을 수 없습니다.")
        return ""
    except Exception as e:
        print(f"S3 파일 읽기 중 오류 발생: {e}")
        return ""

def write_file_to_s3(bucket_name: str, file_key: str, content: str):
    """
    S3에 JSON 파일을 저장합니다.

    Args:
        bucket_name (str): S3 버킷 이름
        file_key (str): S3에 저장할 파일 경로(Key)
        content (str): 저장할 파일 내용
    """
    try:
        s3 = boto3.client('s3')  # AWS S3 클라이언트 생성
        s3.put_object(Bucket=bucket_name, Key=file_key, Body=content)
        print(f"S3에 파일이 저장되었습니다: s3://{bucket_name}/{file_key}")
    except NoCredentialsError:
        print("AWS 자격 증명을 찾을 수 없습니다.")
    except Exception as e:
        print(f"S3 파일 저장 중 오류 발생: {e}")

def process_json_lines_with_time_conversion_s3(bucket_name: str, input_file_key: str, output_file_key: str):
    """
    S3에서 JSON 파일을 읽어와 UTC -> KST 변환 후 S3에 저장합니다.

    Args:
        bucket_name (str): S3 버킷 이름
        input_file_key (str): 입력 파일 경로(Key)
        output_file_key (str): 출력 파일 경로(Key)
    """
    file_content = read_file_from_s3(bucket_name, input_file_key)
    if not file_content:
        print("입력 파일을 읽지 못했습니다.")
        return

    output_content = []

    for line in file_content.splitlines():
        if line.strip():  # 빈 줄 무시
            try:
                data = json.loads(line)  # JSON 파싱
                if 'requestTime' in data:
                    # UTC -> KST 변환
                    utc_time = data['requestTime']
                    kst_time = convert_utc_to_kst(utc_time)
                    data['requestTime'] = kst_time
                output_content.append(json.dumps(data, ensure_ascii=False))
            except (json.JSONDecodeError, ValueError) as e:
                print(f"JSON 처리 중 오류 발생: {line.strip()} (Error: {e})")

    # S3에 결과 저장
    write_file_to_s3(bucket_name, output_file_key, "\n".join(output_content))

# 기존 UTC -> KST 변환 함수
def convert_utc_to_kst(utc_time: str) -> str:
    if "." in utc_time:
        utc_time, fraction = utc_time.split(".", 1)
        fraction = fraction[:6]  # 최대 6자리까지만 유지 (마이크로초)
        utc_time = f"{utc_time}.{fraction}"

    utc_datetime = datetime.strptime(utc_time, "%Y-%m-%dT%H:%M:%S.%f")
    kst_datetime = utc_datetime + timedelta(hours=9)
    return kst_datetime.isoformat()

if __name__ == "__main__":
    bucket_name = "tripcok"  # S3 버킷 이름
    base_input_file_key = "topics/tripcok/2024-12-25/tripcok+0+000"
    base_output_file_key = "topics/tripcok/2024-12-25/tripcok+0+000"
    start_number = 000
    end_number = 1100
    step = 100

    for current_number in range(start_number, end_number + 1, step):
        print(current_number)
        input_file_key = f"{base_input_file_key}{current_number:07d}.json"  # 입력 파일 경로 생성
        output_file_key = f"{base_output_file_key}{current_number:07d}_plus9.json"  # 출력 파일 경로 생성

        print(f"Processing file: {input_file_key} -> {output_file_key}")

        process_json_lines_with_time_conversion_s3(bucket_name, input_file_key, output_file_key)
