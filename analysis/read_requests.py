import requests
import random
import time
import concurrent.futures
import json
import threading

def make_read_request(docker_url, max_stud_id, read_times_lock, read_times):
    low = random.randint(0, max_stud_id)
    payload = {"Stud_id": {"low": low, "high": low + 50}}
    start_time = time.time()
    res = requests.get(f"{docker_url}/read", json=payload)
    end_time = time.time() - start_time
    if res.status_code == 200:
        with read_times_lock:
            read_times.append(end_time)
    else:
        print(f"Error {res.status_code} in read")

def main():
    docker_url = "http://localhost:5000"
    max_stud_id = 16383
    num_requests = 10000
    read_times_lock = threading.Lock()
    read_times = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        tasks = [executor.submit(make_read_request, docker_url, max_stud_id, read_times_lock, read_times) for _ in range(num_requests)]
        concurrent.futures.wait(tasks)

    with open('read_times.json', 'w') as f:
        json.dump(read_times, f)

if __name__ == "__main__":
    main()
