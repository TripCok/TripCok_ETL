


# 실행 예제
if __name__ == "__main__":
    # KST 시간대 정의
    KST = timezone(timedelta(hours=9))

    # 현재 KST 날짜 가져오기
    current_date = datetime.now(KST).strftime('%Y%m%d')
    
    # 명령줄 인자 처리 
    parser = argparse.ArgumentParser(description="Logs Cleansing Pipeline")
    parser.add_argument("--bucket", required=False, default="tripcok", help="S3 bucket name")
    parser.add_argument("--folder", required=False, default=f"processed_data/", help="S3 folder path (prefix)")
    parser.add_argument("--date", required=True, help="Execution date (YYYYMMDD)")
    
    # parser 저장
    args = parser.parse_args()

    app = InsertDb(bucket_name=args.bucket, folder_path=args.folder, execute_date=args.date)
    app.run()
