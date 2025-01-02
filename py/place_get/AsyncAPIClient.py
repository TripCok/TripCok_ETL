import os
import asyncio
from aiohttp import ClientSession

class AsyncAPIClient:
    def __init__(self):
        model_server = os.getenv("MODEL_SERVER")
        model_port = os.getenv("MODEL_PORT")

        self.url = f"http://{model_server}:{model_port}"

    @staticmethod
    async def fetch(url, payload):
        async with ClientSession() as session:
            async with session.post(url, json=payload) as response:
                return await response.json()

    async def get_recommendations(ass, data):
        print("get_recommendations 진입")
        tasks = []
        model_server = os.getenv("MODEL_SERVER")
        model_port = os.getenv("MODEL_PORT")
        url = f"http://{model_server}:{model_port}" + "/recommend"

        for ml_mapping in data:
            payload = {"contentids": [ml_mapping], "top_k": 5}
            tasks.append((ml_mapping, asyncio.create_task(ass.fetch(url, payload))))

        results = []
        for ml_mapping, task in tasks:
            result = await task
            print(result)
            results.append({"ml_mapping_id": ml_mapping, "recommendations": result})

        return results


