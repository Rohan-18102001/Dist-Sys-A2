from flask import Flask, jsonify, request
import uuid
import requests
import os
import time
import threading

app = Flask(__name__)

def generate_random_id():
    unique_identifier = uuid.uuid4()
    return unique_identifier.int

class SingletonMeta(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            with threading.Lock():
                if cls not in cls._instances:
                    instance = super().__call__(*args, **kwargs)
                    cls._instances[cls] = instance
                    instance.lock_dict = {}
                    instance.global_lock = threading.Lock()
        return cls._instances[cls]

class MultiLockDict(metaclass=SingletonMeta):
    def acquire_lock(self, identifier):
        with self.global_lock:
            if identifier not in self.lock_dict:
                self.lock_dict[identifier] = threading.Lock()
        self.lock_dict[identifier].acquire()

    def release_lock(self, identifier):
        self.lock_dict[identifier].release()

class Server:
    def __init__(self, identifier, server_name):
        self.server_id = identifier
        self.server_name = server_name
        self.shardsToDB = {}
        self.shards = []

    def addShard(self, shard_identifier, shard_name):
        self.shardsToDB[shard_identifier] = shard_name
        if shard_identifier not in self.shards:
            self.shards.append(shard_identifier)

    def updateData(self, shard_identifier, data):
        payload = {
            "shard": self.shardsToDB[shard_identifier],
            "data": data,
            "Stud_id": data["Stud_id"],
        }
        requests.put(f"http://{self.server_name}:5000/update", json=payload)

    def delData(self, shard_identifier, stud_identifier):
        payload = {
            "shard": self.shardsToDB[shard_identifier],
            "Stud_id": stud_identifier,
        }
        requests.delete(f"http://{self.server_name}:5000/del", json=payload)

    def insertData(self, shard_identifier, data):
        payload = {
            "shard": self.shardsToDB[shard_identifier],
            "data": data,
        }
        requests.post(f"http://{self.server_name}:5000/write", json=payload)

    def getData(self, shard_identifier, id_limits):
        payload = {
            "shard": self.shardsToDB[shard_identifier],
            "Stud_id": id_limits,
        }
        response = requests.get(f"http://{self.server_name}:5000/read", json=payload)
        return response.json().get("data", [])

    def getStatus(self):
        return list(self.shardsToDB.keys())

    def __str__(self):
        result = f"server_id - {self.server_id}\n"
        for key, value in self.shardsToDB.items():
            result += f"{key} - {value}\n"
        return result

class ServerMap:
    _instance = None

    def __init__(self):
        self.nameToIdMap = {}
        self.idToNameMap = {}
        self.idToServer = {}

    @classmethod
    def getInstance(cls):
        if not cls._instance:
            cls._instance = cls()
        return cls._instance

    def addServer(self, server_name):
        unique_id = generate_random_id()
        self._updateMapsOnAdd(server_name, unique_id)
        self.idToServer[unique_id] = Server(unique_id, server_name)

    def _updateMapsOnAdd(self, server_name, unique_id):
        self.nameToIdMap[server_name] = unique_id
        self.idToNameMap[unique_id] = server_name

    def removeServer(self, server_id):
        server_name = self.getNameFromId(server_id)
        self._dockerStopRemove(server_name)
        shardList = self._removeServerDetails(server_id)
        return shardList

    def _dockerStopRemove(self, server_name):
        cmd = f"sudo docker stop {server_name} && sudo docker rm {server_name}"
        res = os.popen(cmd).read()
        if not res:
            raise Exception("<ERROR> Container could not be stopped!")

    def _removeServerDetails(self, server_id):
        server = self.idToServer.pop(server_id, None)
        if server:
            server_name = self.idToNameMap.pop(server_id)
            self.nameToIdMap.pop(server_name, None)
            return list(server.shardsToDB.keys())
        return []

    def addShardToServer(self, server_id, shard_id, shard_name):
        if server_id in self.idToServer:
            self.idToServer[server_id].addShard(shard_id, shard_name)

    def getServersCount(self):
        return len(self.nameToIdMap)

    def getData(self, shardFragment, id_limits):
        server_id = shardFragment["server_id"]
        if server_id in self.idToServer:
            return self.idToServer[server_id].getData(shardFragment["shard_id"], id_limits)

    def getStatus(self, server_id=None):
        if server_id:
            return self.idToServer.get(server_id).getStatus()
        return {name: self.idToServer[id].getStatus() for name, id in self.nameToIdMap.items()}

    def bulkOperations(self, operation, serversList, shard_id, data=None, stud_id=None):
        for server_id in serversList:
            server = self.idToServer.get(server_id)
            if server:
                if operation == 'insert':
                    server.insertData(shard_id, data)
                elif operation == 'update':
                    server.updateData(shard_id, data)
                elif operation == 'delete':
                    server.delData(shard_id, stud_id)

    def __str__(self):
        res = "Maps Overview: \n"
        for name, id in self.nameToIdMap.items():
            res += f"Server Name: {name}, ID: {id}, Details: {self.idToServer[id].__str__()}\n"
        return res



class Shard:

    shard_id = -1
    student_id_low = -1
    shard_size = 0

    RING_SIZE = 512
    VIRTUAL_INSTANCE = 9

    hashRing = []

    def __init__(self, shard_id, student_id_low, shard_size):
        self.shard_id = shard_id
        self.student_id_low = student_id_low
        self.shard_size = shard_size
        self.hashRing = [-1] * self.RING_SIZE

    def isDataPresent(self, id_limits):
        return not (self.student_id_low > id_limits["high"] or self.student_id_low + self.shard_size < id_limits["low"])

    def getStudentIdLow(self):
        return self.student_id_low

    def getShardSize(self):
        return self.shard_size

    def request_hash(self, i):
        return (i * i + 2 * i + 17) % Shard.RING_SIZE

    def virtual_server_hash(self, i, j):
        return (i * i + j * j + 2 * j + 25) % Shard.RING_SIZE

    def vacantRingSpot(self, virtual_hash):
        initial = virtual_hash
        while self.hashRing[virtual_hash] != -1:
            virtual_hash = (virtual_hash + 1) % Shard.RING_SIZE
            if virtual_hash == initial:
                return -1  # Indicates the ring is full, though it should never happen in the given setup.
        return virtual_hash

    def addServer(self, server_id):
        for loop in range(self.VIRTUAL_INSTANCE):
            virtual_hash = self.virtual_server_hash(server_id, loop + 1)
            emptySpot = self.vacantRingSpot(virtual_hash)
            if emptySpot != -1:  # Check if a spot was found.
                self.hashRing[emptySpot] = server_id

    def getLoadBalancedServerId(self, request_id):
        index = request_id % Shard.RING_SIZE
        start = index
        while self.hashRing[index] == -1:
            index = (index + 1) % Shard.RING_SIZE
            if index == start:
                return -1  # Indicates that no server is found.
        return self.hashRing[index]

    def removeServer(self, server_id):
        modified = False
        for idx, value in enumerate(self.hashRing):
            if value == server_id:
                self.hashRing[idx] = -1
                modified = True
        return modified

    def getAllServers(self):
        unique_servers = set(self.hashRing) - {-1}
        return list(unique_servers)

    def __str__(self):
        return f"Shard ID: {self.shard_id}, Student ID Low: {self.student_id_low}, Shard Size: {self.shard_size}"


class ShardMap:
    _instance = None
    nameToIdMap = {}
    idToShard = {}


    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(ShardMap, cls).__new__(cls, *args, **kwargs)
            cls._instance.nameToIdMap = {}
            cls._instance.idToShard = {}
        return cls._instance

    @classmethod
    def getInstance(cls):
        if not cls._instance:
            cls._instance = cls()
        return cls._instance

    def loadBalancedServer(self, shard_name):
        shard_id = self.nameToIdMap.get(shard_name)
        request_id = generate_random_id()
        return self.idToShard[shard_id].loadBalancedServer(request_id)

    def idFromName(self, shard_name):
        return self.nameToIdMap.get(shard_name)

    def serversFromShard(self, shard_id):
        return self.idToShard.get(shard_id).allServers()

    def shardIdForStudent(self, student_id):
        for shard_id, shard in self.idToShard.items():
            if shard.containsData({"low": student_id, "high": student_id}):
                return shard_id

    def shardName(self, shard_id):
        for name, id in self.nameToIdMap.items():
            if id == shard_id:
                return name
        return "NA Shard"

    def registerShard(self, shard_info):
        shard_name = shard_info["Shard_id"]
        if shard_name not in self.nameToIdMap:
            unique_id = generate_random_id()
            self.nameToIdMap[shard_name] = unique_id
            self.idToShard[unique_id] = Shard(unique_id, shard_info["Stud_id_low"], shard_info["Shard_size"])

    def linkServer(self, shard_name, server_id):
        if shard_name in self.nameToIdMap:
            shard = self.idToShard[self.nameToIdMap[shard_name]]
            shard.addServer(server_id)

    def unlinkServer(self, shards, server_id):
        for shard_id in shards:
            if self.idToShard[shard_id].removeServer(server_id) is False:
                shard_name = self.shardName(shard_id)
                del self.nameToIdMap[shard_name]
                del self.idToShard[shard_id]

    def systemStatus(self):
        return [{"Shard_id": name, "Stud_id_low": self.idToShard[id].studentIdLow(), "Shard_size": self.idToShard[id].shardSize()} for name, id in self.nameToIdMap.items()]

    def shardDataSegments(self, id_bounds):
        segments = []
        for shard_id, shard in self.idToShard.items():
            if shard.containsData(id_bounds):
                request_id = generate_random_id()
                segments.append({"shard_id": shard_id, "server_id": shard.loadBalancedServer(request_id)})
        return segments

    def __str__(self):
        nameMap = "\n ".join([f"{name} - {id}," for name, id in self.nameToIdMap.items()])
        shardMap = "\n".join([f"{id} - {shard.__str__()}," for id, shard in self.idToShard.items()])
        return f"NameToID - [\n {nameMap} ] \nIDToShard - [ \n{shardMap} ] \n"

schema = None


   

@app.route("/init", methods=["POST"])
def initialize_database():
    payload = request.get_json()

    # Get instances of ShardMap and ServerMap
    shard_map = ShardMap.getInstance()
    server_map = ServerMap.getInstance()

    # Set global schema based on the payload
    global schema
    schema = payload.get("schema")

    # Initialize shards using the registerShard method
    # Ensure the payload for shards is structured correctly for registerShard
    for shard_info in payload.get("shards", []):
        shard_map.registerShard(shard_info)

    # Setup servers and associate shards
    for server, shard_list in payload.get("servers", {}).items():
        deploy_server(server)
        associate_shards_with_server(server, shard_list, shard_map, server_map)

    return jsonify({"message": "Database Configured", "status": "success"}), 200

def deploy_server(server_name):
    command = f"sudo docker run --platform linux/x86_64 --name {server_name} --network pub --network-alias {server_name} -d ds_server:latest"
    try:
        result = os.popen(command).read()
        if not result:
            raise Exception("Docker command execution failed")
    except Exception as error:
        print(error)

def associate_shards_with_server(server_name, shards, shard_map, server_map):
    server_map.add_server(server_name)
    server_id = server_map.get_id_from_name(server_name)

    shard_ids = [shard_map.add_server_to_shard(shard, server_id) for shard in shards]
    shard_identifiers = [shard_map.get_id_from_name(shard) for shard in shards]
    [server_map.add_shard_to_server(server_id, shard_id, shard) for shard_id, shard in zip(shard_identifiers, shards)]

    send_configuration_request(server_name, {"schema": schema, "shards": shard_ids})

def send_configuration_request(server_name, request_body):
    while True:
        try:
            response = requests.post(f"http://{server_name}:5000/config", json=request_body)
            if response.status_code == 200:
                break
        except Exception as error:
            print(error)
            time.sleep(3)

@app.route("/status", methods=["GET"])
def database_status():
    shard_map = ShardMap.getInstance()
    server_map = ServerMap.getInstance()

    # Use systemStatus instead of get_status
    shard_status = shard_map.systemStatus()
    server_status = server_map.getStatus()

    # Assuming getStatus() returns what you need or you adjust accordingly
    formatted_servers = {key: [shard_map.shardName(shard_id) for shard_id in value] for key, value in server_status.items()}

    return jsonify({"shards": shard_status, "servers": formatted_servers}), 200



@app.route("/add", methods=["POST"])
def add():
    response = {}
    try:
        payload = request.json
        validate_payload(payload)

        serverMap = ServerMap()
        shardMap = ShardMap()
        process_new_shards(payload["new_shards"], shardMap)
        addedServerNames = process_servers(payload, shardMap, serverMap)

        response["N"] = serverMap.getServersCount()
        response["message"] = "Added " + ", ".join(addedServerNames) + " successfully"
        response["status"] = "successful"
        return response, 200

    except Exception as e:
        response = {"message": str(e), "status": "failure"}
        return response, 400


def validate_payload(payload):
    if payload["n"] > len(payload["servers"]):
        raise Exception("<Error> Number of new servers (n) is greater than newly added instances")


def process_new_shards(new_shards, shardMap):
    for shard in new_shards:
        shardMap.addShard(shard)


def process_servers(payload, shardMap, serverMap):
    addedServerNames = []
    for server_name, shards in payload["servers"].items():
        server_name = sanitize_server_name(server_name)
        deploy_server_container(server_name)
        serverMap.addServer(server_name)
        addedServerNames.append(server_name)
        process_shards_for_server(server_name, shards, shardMap, serverMap)
    return addedServerNames


def sanitize_server_name(server_name):
    if "[" in server_name:
        server_name = f"Server{generate_random_id()%10000}"
    return server_name


def deploy_server_container(server_name):
    res = os.popen(f"sudo docker run --platform linux/x86_64 --name {server_name} --network pub --network-alias {server_name} -d ds_server:latest").read()
    if len(res) == 0:
        raise Exception("Deployment failed")


def process_shards_for_server(server_name, shards, shardMap, serverMap):
    server_id = serverMap.getIdFromName(server_name)
    shard_ids = []
    for shard in shards:
        shard_id, shard_data = retrieve_shard_data(shard, shardMap, serverMap)
        if shard_id is not None:
            shard_ids.append(shard)
            shardMap.addServerToShard(shard, server_id)
            serverMap.addShardToServer(server_id, shard_id, shard)
            serverMap.insertBulkData([server_id], shard_id, shard_data)
    configure_server(server_name, shard_ids)


def retrieve_shard_data(shard, shardMap, serverMap):
    mapped_server_id = shardMap.getLoadBalancedServerForShard(shard)
    if mapped_server_id == -1:
        return None, []
    mapped_server_name = serverMap.getNameFromId(mapped_server_id)
    res = requests.get(f"http://{mapped_server_name}:5000/copy", json={"shards": [shard]})
    shard_id = shardMap.getIdFromName(shard)
    return shard_id, [data.pop("id") or data for data in res.json()["message"]]


def configure_server(server_name, shard_ids):
    while True:
        try:
            requests.post(f"http://{server_name}:5000/config", json={"schema": schema, "shards": shard_ids})
            break
        except Exception as e:
            print(e)
            time.sleep(3)


@app.route("/rm", methods=["DELETE"])
def remove():
    try:
        payload = request.json
        serverMap = ServerMap()
        shardMap = ShardMap()
        if payload["n"] < len(payload["servers"]):
            raise Exception(
                "<ERROR> Number of server names should be less than or equal to the number of instances to be removed"
            )

        serversToDel = [serverId for serverName, serverId in serverMap.nameToIdMap.items() if serverName in payload["servers"]]
        serversToDelNames = [serverName for serverName in payload["servers"] if serverName in serverMap.nameToIdMap]

        additionalServers = [serverId for serverName, serverId in serverMap.nameToIdMap.items() if serverId not in serversToDel][:payload["n"] - len(serversToDel)]
        serversToDel.extend(additionalServers)
        serversToDelNames.extend(serverMap.getNameFromId(serverId) for serverId in additionalServers)

        for serverId in serversToDel:
            shardList = serverMap.removeServer(serverId)
            shardMap.removeServerFromShard(shardList, serverId)

        response = {"message": {"N": payload["n"], "servers": serversToDelNames}, "status": "successful"}
        return jsonify(response), 200
    except Exception as e:
        response = {"message": str(e), "status": "failure"}
        return jsonify(response), 400

@app.route("/read", methods=["GET"])
def read():
    try:
        payload = request.json
        studId = payload["Stud_id"]

        shardMap = ShardMap()
        serverMap = ServerMap()

        shardFragments = shardMap.getShardFragments(studId)
        result = []
        for shardFragment in shardFragments:
            server_id = shardFragment["server_id"]
            server_name = serverMap.getNameFromId(server_id)
            try:
                res = requests.get(f"http://{server_name}:5000/heartbeat")
            except:
                shardsInServer = serverMap.getStatus(server_id)
                manageServerFailure(server_name, shardsInServer, shardMap, serverMap)

            updateResultData(shardFragment, studId, result, serverMap)

        response = formatResponse(shardFragments, result, shardMap)
        return jsonify(response), 200
    except Exception as e:
        return jsonify({"message": str(e), "status": "failure"}), 400

def manageServerFailure(server_name, shardsInServer, shardMap, serverMap):
    payload = {"n": 1, "servers": [server_name]}
    requests.delete(f"http://localhost:5000/rm", json=payload)
    payload = {
        "n": 1,
        "new_shards": [],
        "servers": {server_name: [shardMap.getNameFromId(shardId) for shardId in shardsInServer]},
    }
    requests.post(f"http://localhost:5000/add", json=payload)

def updateResultData(shardFragment, studId, result, serverMap):
    server_name = serverMap.getNameFromId(shardFragment["server_id"])
    shardFragment["server_id"] = serverMap.getIdFromName(server_name)
    data = serverMap.getData(shardFragment, studId)
    for item in data:
        item.pop("id")
        result.append(item)

def formatResponse(shardFragments, result, shardMap):
    response = {"shards_queried": [shardMap.getNameFromId(shardFragment["shard_id"]) for shardFragment in shardFragments], "data": result, "status": "success"}
    return response


@app.route("/write", methods=["POST"])
def write():
    try:
        input_data = request.json
        shard_data_mapping = {}
        shard_mapper = ShardMap()
        server_mapper = ServerMap()

        # Aggregate data by shard ID
        for record in input_data["data"]:
            shard_key = shard_mapper.getShardIdFromStudId(record["Stud_id"])
            shard_data_mapping.setdefault(shard_key, []).append(record)

        locker = MultiLockDict()

        # Process each shard's data
        for shard_key, records in shard_data_mapping.items():
            with locker.lock(shard_key):
                try:
                    target_servers = shard_mapper.getAllServersFromShardId(shard_key)
                    server_mapper.insertBulkData(target_servers, shard_key, records)
                except Exception as err:
                    return {"message": str(err), "status": "failure"}, 400

        return {"message": f"{len(input_data['data'])} Data entries added", "status": "success"}, 200
    except Exception as exc:
        return {"message": str(exc), "status": "failure"}, 400

@app.route("/update", methods=["PUT"])
def update():
    try:
        input_data = request.json
        if input_data["Stud_id"] != input_data["data"]["Stud_id"]:
            raise ValueError("Student ID mismatch.")

        shard_mapper = ShardMap()
        server_mapper = ServerMap()

        shard_key = shard_mapper.getShardIdFromStudId(input_data["Stud_id"])
        locker = MultiLockDict()

        with locker.lock(shard_key):
            server_targets = shard_mapper.getAllServersFromShardId(shard_key)
            server_mapper.updateData(server_targets, shard_key, input_data["data"])

        return {"message": "Student data updated successfully.", "status": "success"}, 200
    except Exception as err:
        return {"message": str(err), "status": "failure"}, 400

@app.route("/del", methods=["DELETE"])
def delete():
    try:
        input_data = request.json
        shard_mapper = ShardMap()
        server_mapper = ServerMap()

        shard_key = shard_mapper.getShardIdFromStudId(input_data["Stud_id"])
        locker = MultiLockDict()

        with locker.lock(shard_key):
            server_targets = shard_mapper.getAllServersFromShardId(shard_key)
            server_mapper.delData(server_targets, shard_key, input_data["Stud_id"])

        return {"message": "Student data removed successfully.", "status": "success"}, 200
    except Exception as err:
        return {"message": str(err), "status": "failure"}, 400



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
