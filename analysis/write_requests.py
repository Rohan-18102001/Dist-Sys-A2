import requests
import random
import time
import concurrent.futures
import json
import threading

def make_write_request(docker_url, max_stud_id, write_times_lock, write_times):
    payload = {
        "data": [
            {
                "Stud_id": random.randint(0, max_stud_id),
                "Stud_name": "Bhupendra Jogi",
                "Stud_marks": random.randint(0, 100),
            },
        ]
    }
    start_time = time.time()
    res = requests.post(f"{docker_url}/write", json=payload)
    end_time = time.time() - start_time
    if res.status_code == 200:
        with write_times_lock:
            write_times.append(end_time)
    else:
        print(f"Error {res.status_code} in write")

def main():
    docker_url = "http://localhost:5000"
    max_stud_id = 16383
    num_requests = 10000
    write_times_lock = threading.Lock()
    write_times = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        tasks = [executor.submit(make_write_request, docker_url, max_stud_id, write_times_lock, write_times) for _ in range(num_requests)]
        concurrent.futures.wait(tasks)

    with open('write_times.json', 'w') as f:
        json.dump(write_times, f)

if __name__ == "__main__":
    main()
