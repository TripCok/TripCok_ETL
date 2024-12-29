import boto3

# S3 클라이언트 생성
s3 = boto3.client('s3')

# 버킷 이름과 탐색할 경로
bucket_name = 'tripcok'

rename_folders = [
    'topics/tripcok/2024-12-25/',
    'topics/tripcok/2024-12-26/',
    'topics/tripcok/2024-12-27/',
]


def rename_file(old_key: str):
    new_key = old_key.replace('_plus9', '')
    print(f"Renaming {old_key} to {new_key}")
    return new_key


def rollback_changes(bucket, changes):
    """
    롤백 처리 함수
    :param bucket: S3 버킷 이름
    :param changes: 복구할 파일의 변경 기록 (리스트: [(old_key, new_key)])
    """
    print("롤백을 시작합니다...")
    for old_key, new_key in reversed(changes):
        try:
            # 새 이름으로 복사된 객체를 원래 이름으로 복원
            s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': new_key}, Key=old_key)
            # 잘못된 새 객체 삭제
            s3.delete_object(Bucket=bucket, Key=new_key)
            print(f"복구 완료: {new_key} -> {old_key}")
        except Exception as e:
            print(f"롤백 실패: {new_key} -> {old_key}, 오류: {e}")


# 파일 이름 변경 함수
def rename_files_run(bucket, prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    if 'Contents' not in response:
        print("지정된 경로에 파일이 없습니다.")
        return

    # 처리해야 할 파일의 총 개수
    process_size = len(response['Contents'])

    # 성공적으로 처리된 개수와 변경 기록 저장
    success_process = 0
    changes = []

    try:
        for obj in response['Contents']:
            old_key = obj['Key']
            # '_plus9' 부분이 있는 파일만 처리
            if '_plus9' in old_key:
                new_key = rename_file(old_key)
                # 새 이름으로 객체 복사
                s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': old_key}, Key=new_key)
                # 기존 객체 삭제
                s3.delete_object(Bucket=bucket, Key=old_key)
                # 변경 기록 저장
                changes.append((old_key, new_key))
                success_process += 1

        # 모든 파일 처리 완료
        if success_process == process_size:
            print(f"{prefix}의 모든 데이터 처리 완료")

    except Exception as e:
        print(f"오류 발생: {e}")
        # 오류 발생 시 롤백
        rollback_changes(bucket, changes)


# 함수 호출
if __name__ == '__main__':
    for folder in rename_folders:
        rename_files_run(bucket_name, folder)
