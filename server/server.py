from flask import Flask, jsonify, request
from threading import Thread
import pymysql
from queue import Queue

# Database.py content
class DBAccess:
    def __init__(self, schema=None, data_types=None, DB_connector=None, entity="StudentRecords"):
        self.schema = schema
        self.data_types = data_types
        self.DB_connector = DB_connector
        self._prepare(entity)

    def _prepare(self, entity):
        self.entity = self.DB_connector.executor.submit(
            self.DB_connector.checkEntity, entity, self.schema, self.data_types
        ).result()

    def addRecord(self, data):
        record_id = self.DB_connector.executor.submit(
            self.DB_connector.addData, self.entity, data
        ).result()
        return record_id

    def addMultiple(self, data_list):
        ids = []
        for data in data_list:
            ids.append(self.DB_connector.executor.submit(
                self.DB_connector.addData, self.entity, data
            ).result())
        return ids[-1]

    def fetchAll(self):
        return self.DB_connector.executor.submit(
            self.DB_connector.retrieveAll, self.entity
        ).result()

    def fetchSubset(self, start, end):
        return self.DB_connector.executor.submit(
            self.DB_connector.retrieveRange, self.entity, start, end
        ).result()

    def modify(self, record_id, data):
        return self.DB_connector.executor.submit(
            self.DB_connector.modifyData, self.entity, record_id, data
        ).result()

    def remove(self, record_id):
        return self.DB_connector.executor.submit(
            self.DB_connector.removeData, self.entity, record_id
        ).result()

class DBConnection:
    def __init__(self, host="localhost", username="user", password="pass", database="db1"):
        self.executor = ThreadPoolExecutor(max_workers=1)
        self.host = host
        self.username = username
        self.password = password
        self.database = database
        self._connect()

    def _connect(self):
        while True:
            try:
                self.connection = pymysql.connect(
                    host=self.host, user=self.username, password=self.password, db=self.database
                )
                break
            except Exception as e:
                print("Connection error:", e)

    def execute(self, query):
        try:
            cursor = self.connection.cursor(pymysql.cursors.DictCursor)
            cursor.execute(query)
        except Exception as reconnect:
            self._connect()
            cursor = self.connection.cursor(pymysql.cursors.DictCursor)
            cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        self.connection.commit()
        return result

    def checkEntity(self, entity_name, schema, data_types):
        entities = self.execute("SHOW TABLES")
        if entity_name not in [e[f"Tables_in_{self.database}"] for e in entities]:
            columns_def = ", ".join([f"{field} {data_types[i]}" for i, field in enumerate(schema)])
            self.execute(f"CREATE TABLE {entity_name} (id INT AUTO_INCREMENT PRIMARY KEY, {columns_def})")
        return entity_name

    def addData(self, entity, data):
        keys = ", ".join(data.keys())
        values = ", ".join([f"'{v}'" for v in data.values()])
        self.execute(f"INSERT INTO {entity} ({keys}) VALUES ({values})")
        return self.execute(f"SELECT LAST_INSERT_ID() as id")[0]['id']

    def retrieveAll(self, entity):
        return self.execute(f"SELECT * FROM {entity}")

    def retrieveRange(self, entity, start, end):
        return self.execute(f"SELECT * FROM {entity} WHERE id BETWEEN {start} AND {end}")

    def modifyData(self, entity, record_id, data):
        updates = ", ".join([f"{k}='{v}'" for k, v in data.items()])
        self.execute(f"UPDATE {entity} SET {updates} WHERE id={record_id}")

    def removeData(self, entity, record_id):
        self.execute(f"DELETE FROM {entity} WHERE id={record_id}")

# Application.py content
app = Flask(__name__)
db_sessions = {}

@app.route("/setup", methods=["POST"])
def setup():
    details = request.json
    response_text = ""
    for shard in details["shards"]:
        db_sessions[shard] = DBAccess(
            schema=details["schema"]["fields"], 
            data_types=details["schema"]["types"],
    DB_connector=DBConnection(database=shard))
    response_text += f"Configured shard: {shard}, "
    response_text += "all set up."
    return jsonify({"message": response_text, "status": "completed"}), 200

@app.route("/add", methods=["POST"])
def add_data():
    info = request.json
    shard = info["shard"]
    data = info["data"]
    record_id = db_sessions[shard].addRecord(data)
    return jsonify({"message": "Data added", "record_id": record_id, "status": "success"}), 200

@app.route("/bulk_add", methods=["POST"])
def bulk_add():
    info = request.json
    shard = info["shard"]
    data_list = info["data"]
    last_id = db_sessions[shard].addMultiple(data_list)
    return jsonify({"message": "Bulk data added", "last_record_id": last_id, "status": "success"}), 200

@app.route("/fetch", methods=["GET"])
def fetch_all():
    shard = request.args.get("shard")
    records = db_sessions[shard].fetchAll()
    return jsonify({"data": records, "status": "success"}), 200

@app.route("/fetch_range", methods=["GET"])
def fetch_range():
    shard = request.args.get("shard")
    start = int(request.args.get("start"))
    end = int(request.args.get("end"))
    records = db_sessions[shard].fetchSubset(start, end)
    return jsonify({"data": records, "status": "success"}), 200

@app.route("/update", methods=["PUT"])
def update_data():
    info = request.json
    shard = info["shard"]
    record_id = info["record_id"]
    new_data = info["data"]
    db_sessions[shard].modify(record_id, new_data)
    return jsonify({"message": f"Record {record_id} updated", "status": "success"}), 200

@app.route("/delete", methods=["DELETE"])
def delete_data():
    info = request.json
    shard = info["shard"]
    record_id = info["record_id"]
    db_sessions[shard].remove(record_id)
    return jsonify({"message": f"Record {record_id} removed", "status": "success"}), 200

@app.route("/status", methods=["GET"])
def status():
    return jsonify({"message": "Service is running", "status": "active"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)