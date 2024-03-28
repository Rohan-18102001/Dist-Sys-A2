import requests
import random
import time
import concurrent.futures
import json
import threading

def init_database(payload):
    url = "http://localhost:5000"
    res = requests.post(f"{url}/init", json=payload)
    print(res.json())

def make_write_request(docker_url, max_stud_id, write_times):
    payload = {
        "data": [
            {
                "Stud_id": random.randint(0, max_stud_id),
                "Stud_name": "Student Name",
                "Stud_marks": random.randint(0, 100),
            },
        ]
    }
    start_time = time.time()
    res = requests.post(f"{docker_url}/write", json=payload)
    end_time = time.time() - start_time
    if res.status_code == 200:
        write_times.append(end_time)
    else:
        print(f"Error {res.status_code} in write")

def make_read_request(docker_url, max_stud_id, read_times):
    low = random.randint(0, max_stud_id)
    payload = {"Stud_id": {"low": low, "high": low + 50}}
    start_time = time.time()
    res = requests.get(f"{docker_url}/read", json=payload)
    end_time = time.time() - start_time
    if res.status_code == 200:
        read_times.append(end_time)
    else:
        print(f"Error {res.status_code} in read")

def perform_requests(config, max_stud_id):
    docker_url = "http://localhost:5000"
    num_requests = 10000
    write_times = []
    read_times = []

    # Perform write requests
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        tasks = [executor.submit(make_write_request, docker_url, max_stud_id, write_times) for _ in range(num_requests)]
        concurrent.futures.wait(tasks)

    # Perform read requests
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        tasks = [executor.submit(make_read_request, docker_url, max_stud_id, read_times) for _ in range(num_requests)]
        concurrent.futures.wait(tasks)

    # Calculate the average times
    average_write_time = sum(write_times) / len(write_times) if write_times else 0
    average_read_time = sum(read_times) / len(read_times) if read_times else 0

    print(f"Configuration {config}: Average write time = {average_write_time}, Average read time = {average_read_time}")
    return average_write_time, average_read_time

def main():
    payloads = {
        'a1': {
    "N": 6,
    "schema": {
        "columns": ["Stud_id", "Stud_name", "Stud_marks"],
        "dtypes": ["Number", "String", "Number"],
    },
    "shards": [
        {"Stud_id_low": 0, "Shard_id": "sh1", "Shard_size": 4096},
        {"Stud_id_low": 4096, "Shard_id": "sh2", "Shard_size": 4096},
        {"Stud_id_low": 8192, "Shard_id": "sh3", "Shard_size": 4096},
        {"Stud_id_low": 12288, "Shard_id": "sh4", "Shard_size": 4096},
    ],
    "servers": {
        "Server0": ["sh1", "sh2"],
        "Server1": ["sh3", "sh4"],
        "Server2": ["sh1", "sh3"],
        "Server3": ["sh4", "sh2"],
        "Server4": ["sh1", "sh4"],
        "Server5": ["sh3", "sh2"],
    },
},
        'a2': {
    "N": 6,
    "schema": {
        "columns": ["Stud_id", "Stud_name", "Stud_marks"],
        "dtypes": ["Number", "String", "Number"],
    },
    "shards": [
        {"Stud_id_low": 0, "Shard_id": "sh1", "Shard_size": 4096},
        {"Stud_id_low": 4096, "Shard_id": "sh2", "Shard_size": 4096},
        {"Stud_id_low": 8192, "Shard_id": "sh3", "Shard_size": 4096},
        {"Stud_id_low": 12288, "Shard_id": "sh4", "Shard_size": 4096},
    ],
    "servers": {
        "Server0": ["sh1", "sh2", "sh3", "sh4"],
        "Server1": ["sh1", "sh2", "sh3", "sh4"],
        "Server2": ["sh1", "sh2", "sh3", "sh4"],
        "Server3": ["sh1", "sh2", "sh3", "sh4"],
        "Server4": ["sh1", "sh2", "sh3", "sh4"],
        "Server5": ["sh1", "sh2", "sh3", "sh4"],
    },
},
        'a3': {
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
},
    }

    results = {}

    for config, payload in payloads.items():
        init_database(payload)
        results[config] = perform_requests(config, payload['shards'][-1]['Stud_id_low'] + payload['shards'][-1]['Shard_size'] - 1)

    # Calculate the speedups
    speedup_write_a2_from_a1 = results['a1'][0] / results['a2'][0]
    speedup_write_a3_from_a1 = results['a1'][0] / results['a3'][0]
    speedup_read_a2_from_a1 = results['a1'][1] / results['a2'][1]
    speedup_read_a3_from_a1 = results['a1'][1] / results['a3'][1]

    print(f"Write speedup of a2 from a1: {speedup_write_a2_from_a1}")
    print(f"Write speedup of a3 from a1: {speedup_write_a3_from_a1}")
    print(f"Read speedup of a2 from a1: {speedup_read_a2_from_a1}")
    print(f"Read speedup of a3 from a1: {speedup_read_a3_from_a1}")

if __name__ == "__main__":
    main()
