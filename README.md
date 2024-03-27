# Distributed Systems Assignment 2

## Prerequisites

- **docker**: latest version


## Setup

1. Set neccessary permission for the setup and clean script:
   ```
   chmod +x build_and_start.sh 
   ```

   ```
   chmod +x clean.sh 
   ```

2. Setting up Load Balancer:
   ```
   ./build_and_start
   ```

2. Cleaning up resources:
   ```
   ./clean
   ```

## Payload for testing different endpoints of the loadbalancer

### /init

    ```
    {
            "N": 6,
            "schema": {
                "columns": ["Stud_id", "Stud_name", "Stud_marks"],
                "dtypes": ["Number", "String", "Number"]
            },
            "shards": [
                {"Stud_id_low": 0, "Shard_id": "sh1", "Shard_size": 4096},
                {"Stud_id_low": 4096, "Shard_id": "sh2", "Shard_size": 4096},
                {"Stud_id_low": 8192, "Shard_id": "sh3", "Shard_size": 4096},
                {"Stud_id_low": 12288, "Shard_id": "sh4", "Shard_size": 4096}
            ],
            "servers": {
                "Server0": ["sh1", "sh2"],
                "Server1": ["sh3", "sh4"],
                "Server2": ["sh1", "sh3"],
                "Server3": ["sh4", "sh2"],
                "Server4": ["sh1", "sh4"],
                "Server5": ["sh3", "sh2"]
            }
        }
    ```
    

### /add 

    ```
    {
        "n": 2,
        "new_shards": [
            {"Stud_id_low": 12288, "Shard_id": "sh5", "Shard_size": 4096}
        ],
        "servers": {
            "Server4": ["sh3", "sh5"],
            "Server5": ["sh2", "sh5"]
        }
    }
    ```

    wrong input - 
    ```
    {
        "n": 3,
        "new_shards": [
            {"Stud_id_low": 12288, "Shard_id": "sh5", "Shard_size": 4096}
        ],
        "servers": {
            "Server4": ["sh3", "sh5"],
            "Server5": ["sh2", "sh5"]
        }
    }
    ```

### /rm

    ```
    {
        "n": 2,
        "servers": ["Server4"]
    }
    ```

    wrong input - 
    ```
    {
        "n": 2,
        "servers": ["Server1", "Server2", "Server3"]
    }
    ```

### /write 

    ```
    {
    "data": [
        {
            "Stud_id": 4297,
            "Stud_name": "Saurav",
            "Stud_marks": 74
        },
        {
            "Stud_id": 14490,
            "Stud_name": "Sumit",
            "Stud_marks": 87
        },
        {
            "Stud_id": 12915,
            "Stud_name": "Rohan",
            "Stud_marks": 64
        },
        {
            "Stud_id": 2531,
            "Stud_name": "ABC",
            "Stud_marks": 2
        },
        {
            "Stud_id": 15393,
            "Stud_name": "DEF",
            "Stud_marks": 40
        },
        {
            "Stud_id": 217,
            "Stud_name": "GHI",
            "Stud_marks": 16
        },
        {
            "Stud_id": 14047,
            "Stud_name": "KLM",
            "Stud_marks": 95
        }
    ]
    }
    ```

### /read

    ```
    {
        "Stud_id": {
            "low": 2531,
            "high": 14490
        }
    }
    ```

### /update 

    ```
    {
        "Stud_id": 12915,
        "data": {
            "Stud_id": 12915,
            "Stud_name": "Rohan",
            "Stud_marks": 100
        }
    }
    ```

### /del

```
    {
        "Stud_id": 12915
    }
    ```


## Analysis

### A-1: 6 Servers, 4 Shards, 3 Replicas

![image](https://raw.githubusercontent.com/Rohan-18102001/Dist-Sys-A2/master/analysis/write(a1).png)
![image](https://raw.githubusercontent.com/Rohan-18102001/Dist-Sys-A2/master/analysis/read(a1).png)


### A-2: 6 Servers, 4 Shards, 6 Replicas

![image](https://raw.githubusercontent.com/Rohan-18102001/Dist-Sys-A2/master/analysis/write(a2).png)
![image](https://raw.githubusercontent.com/Rohan-18102001/Dist-Sys-A2/master/analysis/read(a2).png)


### A-3: 10 Servers, 6 Shards, 8 Replicas

![image](https://raw.githubusercontent.com/Rohan-18102001/Dist-Sys-A2/master/analysis/write(a3).png)
![image](https://raw.githubusercontent.com/Rohan-18102001/Dist-Sys-A2/master/analysis/read(a3).png)
