#!/bin/bash

# Clean up Docker containers, networks, and images
sudo docker compose down --rmi all
sudo docker stop $(sudo docker ps -a -q)
sudo docker rm $(sudo docker ps -a -q)
sudo docker network rm pub
sudo docker image rm ds_server:latest
