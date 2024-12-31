import asyncio
from place_get.AsyncAPIClient import AsyncAPIClient
from place_get.MemberPlaceRecommend import MemberPlaceRecommend

ass = AsyncAPIClient()
execute_date = "2024-12-27"  # 실제 실행 시 argparse 등으로 동적으로 설정 가능
mpr = MemberPlaceRecommend(execute_date)

df = mpr.load()
df.show(truncate=False)

mpr.fetch_and_save_results(ass,df)

data = df.select("ml_mapping_id").distinct().collect()

data = [row.ml_mapping_id for row in data]
api_results = asyncio.get_event_loop().run_until_complete(
    mpr.fetch_data_in_batches(ass ,data)
)

print(api_results)