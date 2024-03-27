#!/bin/bash

# Build and start Docker containers
sudo docker build -t ds_server:latest ./server
sudo docker compose up -d
