README

### 
LogsCleansing 사용법
```bash
$ python LogsCleansing.py --bucket tripcok --folder topics/tripcoklogs --date 20241220

$ python LogsCleansing.py --help
usage: LogsCleansing.py [-h] [--bucket BUCKET] --folder FOLDER --date DATE

Logs Cleansing Pipeline

options:
  -h, --help       show this help message and exit
  --bucket BUCKET  S3 bucket name
  --folder FOLDER  S3 folder path (prefix)
  --date DATE      Execution date (YYYYMMDD)


```
