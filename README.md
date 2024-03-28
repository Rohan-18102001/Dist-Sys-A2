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

![write(a1)](https://github.com/Rohan-18102001/Dist-Sys-A2/assets/61150756/e3ffbb2c-dc37-4c15-b1c9-fdaac78e8a13)


![read(a1)](https://github.com/Rohan-18102001/Dist-Sys-A2/assets/61150756/d0b2ac92-734d-439f-80b6-863e31f6227e)


### A-2: 6 Servers, 4 Shards, 6 Replicas

![write(a2)](https://github.com/Rohan-18102001/Dist-Sys-A2/assets/61150756/54c04f39-6cc1-4270-a749-b9587555ecc7)


![read(a2)](https://github.com/Rohan-18102001/Dist-Sys-A2/assets/61150756/2c05422e-9445-42e6-a051-aa9d8491a6b2)


### A-3: 10 Servers, 6 Shards, 8 Replicas

![write(a3)](https://github.com/Rohan-18102001/Dist-Sys-A2/assets/61150756/3907667d-fdfd-440b-8a3a-b463e69c7361)


![read(a3)](https://github.com/Rohan-18102001/Dist-Sys-A2/assets/61150756/af9a8228-9b69-41bc-a8f5-c4a8c0b579f1)

### Speeup Calculations
![WhatsApp Image 2024-03-28 at 2 37 13 PM(1)](https://github.com/Rohan-18102001/Dist-Sys-A2/assets/41463018/d541b767-86b9-47d2-8235-26e23d05a448)
![WhatsApp Image 2024-03-28 at 2 37 13 PM](https://github.com/Rohan-18102001/Dist-Sys-A2/assets/41463018/adf524cf-678e-4ab7-a7fc-d704fb53670f)
