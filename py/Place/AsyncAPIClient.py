import os
import asyncio
from aiohttp import ClientSession
from asyncio import Semaphore


class AsyncAPIClient:
    def __init__(self, max_concurrent_requests=5):
        model_server = os.getenv("MODEL_SERVER")
        model_port = os.getenv("MODEL_PORT")

        self.url = f"http://{model_server}:{model_port}"
        self.semaphore = Semaphore(max_concurrent_requests)  # 동시 요청 수 제한

    @staticmethod
    async def fetch(url, payload):
        async with ClientSession() as session:
            async with session.post(url, json=payload) as response:
                return await response.json()


    async def limited_fetch(self, url, payload):
        async with self.semaphore:  # Semaphore로 동시 요청 제한
            return await self.fetch(url, payload)

    async def get_recommendations(ass, data):
        print("get_recommendations 진입")
        tasks = []
        model_server = os.getenv("MODEL_SERVER")
        model_port = os.getenv("MODEL_PORT")
        url = f"http://{model_server}:{model_port}" + "/recommend"
        print(data)
        #병렬 처리를 위해 리스트에 append
        for memberId, ml_mapping in data:
            payload = {"memberId" : memberId ,"contentids": [ml_mapping], "top_k": 5}
            tasks.append((ml_mapping, asyncio.create_task(ass.limited_fetch(url, payload))))

        # 결과값 반환
        results = []
        for ml_mapping, task in tasks:
            result = await task
            print(result)
            results.append({
                "memberId": result.get("memberId"),
                "ml_mapping_id": ml_mapping,
                "recommendations": result.get("results")
            })
        return results


