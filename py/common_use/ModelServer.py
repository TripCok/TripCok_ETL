import os
import requests

class ModelServer(object):
    def __init__(self):

        model_server = os.getenv("MODEL_SERVER")
        model_port = os.getenv("MODEL_PORT")

        self.url = f"http://{model_server}:{model_port}"

    def request2server(self, api="/recommends", param=2750144, body=None, test=False):
        """
        Response: {
            "results": [
                {"cid": 101, "score": 0.9},
                {"cid": 102, "score": 0.5},
                {"cid": 103, "score": 0.3333333333333333}
            ]
        }
        """

        if test:
            return [{"127480" : 0.7},{"2750143":0.6},{"2805408":0.5},{"2750144":0.4},{"2901530":0.3}]

        response = requests.get(self.url+api, params=self.params, json=body)
        if response.status_code == 200:
            print("Response:", response.json())
        else:
            print(f"Failed to get response: {response.status_code}")
        results = response.json()
        return results

    def request2server_list(self, api="/recommends", param=None, body=None, test=False):
        """
        Response: {
            "results": [
                {"cid": 101, "score": 0.9},
                {"cid": 102, "score": 0.5},
                {"cid": 103, "score": 0.3333333333333333}
            ]
        }
        """


        if test:
            return ["127480","2750143","2805408","2750144","2901530"],[0.7,0.6,0.5,0.4,0.3]

        response = requests.get(self.url+api, params=param, json=body)
        if response.status_code == 200:
            print("Response:", response.json())
        else:
            print(f"Failed to get response: {response.status_code}")
        results = response.json()

        cids = [item["cid"] for item in results]
        scores = [item["score"] for item in results]

        return cids, scores

