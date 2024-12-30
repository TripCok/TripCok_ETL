import os
import json
import requests

class ModelServer(object):
    def __init__(self):

        model_server = os.getenv("MODEL_SERVER")
        model_port = os.getenv("MODEL_PORT")

        self.url = f"http://{model_server}:{model_port}"

    def request2server(self, api="/recommends", param=2750144, body=None, test=False):
        """
        {
            "results": [
                {
                    "2698818": 0.6768622398376465,
                    "2751008": 0.652495265007019,
                    "2864631": 0.656969428062439,
                    "3354975": 0.6680836081504822,
                    "3375828": 0.6880638003349304
                }
            ]
        }
        """
        if test:
            return [{"127480" : 0.7,"2750143":0.6,"2805408":0.5, "2750144":0.4, "2901530":0.3}]

        param = [param]
        body = {
            "contentids": param,
            "top_k": 5
        }

        response = requests.post(self.url+api, json=body)
        if response.status_code == 200:
            print("Response:", response.json())
        else:
            print(f"Failed to get response: {response.status_code}")
        results = response.json()
        return results

    # def request2server_list(self, api="/recommends", param=None, body=None, test=False):
    #     """
    #     Response: {
    #         "results": [
    #             {"cid": 101, "score": 0.9},
    #             {"cid": 102, "score": 0.5},
    #             {"cid": 103, "score": 0.3333333333333333}
    #         ]
    #     }
    #     """
    #
    #
    #     if test:
    #         return ["127480","2750143","2805408","2750144","2901530"],[0.7,0.6,0.5,0.4,0.3]
    #
    #     response = requests.get(self.url+api, params=param, json=body)
    #     if response.status_code == 200:
    #         print("Response:", response.json())
    #     else:
    #         print(f"Failed to get response: {response.status_code}")
    #     results = response.json()
    #
    #     cids = [item["cid"] for item in results]
    #     scores = [item["score"] for item in results]
    #
    #     return cids, scores

