import requests

def init_database(payload):
    url = "http://localhost:5000"
    res = requests.post(f"{url}/init", json=payload)
    print(res.json())

if __name__ == "__main__":
    payload = {
        "N": 10,
        "schema": {
            "columns": ["Stud_id", "Stud_name", "Stud_marks"],
            "dtypes": ["Number", "String", "Number"],
        },
        "shards": [
            {"Stud_id_low": 0, "Shard_id": "sh1", "Shard_size": 4096},
            {"Stud_id_low": 4096, "Shard_id": "sh2", "Shard_size": 4096},
            {"Stud_id_low": 8192, "Shard_id": "sh3", "Shard_size": 4096},
            {"Stud_id_low": 12288, "Shard_id": "sh4", "Shard_size": 4096},
            {"Stud_id_low": 16384, "Shard_id": "sh5", "Shard_size": 4096},
            {"Stud_id_low": 20480, "Shard_id": "sh6", "Shard_size": 4096},
        ],
        "servers": {
            "Server0": ["sh1", "sh2", "sh4", "sh6"],
            "Server1": ["sh1", "sh2", "sh3", "sh4", "sh5"],
            "Server2": ["sh1", "sh2", "sh3", "sh5", "sh6"],
            "Server3": ["sh4", "sh2", "sh3", "sh5", "sh6"],
            "Server4": ["sh1", "sh4", "sh5", "sh6"],
            "Server5": ["sh3", "sh2", "sh5", "sh6"],
            "Server6": ["sh1", "sh3", "sh4", "sh5", "sh6"],
            "Server7": ["sh1", "sh3", "sh4", "sh2", "sh5"],
            "Server8": ["sh1", "sh2", "sh3", "sh4", "sh6"],
            "Server9": ["sh1", "sh2", "sh3", "sh4", "sh5", "sh6"],
        },
    }
    init_database(payload)
