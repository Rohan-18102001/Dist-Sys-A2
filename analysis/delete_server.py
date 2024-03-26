import requests

docker_url = "http://localhost:5000"

payload = {
    "n": 6,
    "servers": ["Server0", "Server1", "Server2", "Server3", "Server4", "Server5"],
}
res = requests.delete(f"{docker_url}/rm", json=payload)