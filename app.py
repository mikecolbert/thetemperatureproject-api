import os
import logging
from datetime import datetime
from decimal import Decimal

from flask import Flask, jsonify, request
import pymysql
from pymysql.cursors import DictCursor

from dotenv import load_dotenv

# -----------------------------------------------------------------------------
# Config & Logging
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

logging.info("Loading variables from environment")

load_dotenv()

# DB config from env
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")
DB_SSL_CA = os.getenv(
    "DB_SSL_CA", "./combined-ca-certificates.pem"
)  # optional; can be None

app = Flask(__name__)


# -----------------------------------------------------------------------------
# DB helper (modeled on your class, with a few safety tweaks)
# -----------------------------------------------------------------------------
class DB:
    def __init__(self, host, username, password, dbname, ssl_ca=None):
        self.host = host
        self.username = username
        self.password = password
        self.dbname = dbname
        self.ssl = {"ca": ssl_ca} if ssl_ca else None
        self.conn = None

    def __connect__(self):
        """Connect (or reconnect) to MySQL."""
        try:
            if self.conn is None or not self.conn.open:
                self.conn = pymysql.connect(
                    host=self.host,
                    user=self.username,
                    password=self.password,
                    db=self.dbname,
                    ssl=self.ssl,
                    cursorclass=DictCursor,
                    autocommit=False,  # we control commits on writes
                    charset="utf8mb4",
                )
            else:
                # ensure connection is alive
                self.conn.ping(reconnect=True)
            logging.info("DB connected")
        except pymysql.Error as e:
            logging.error(f"Error connecting to the database: {e}")
            raise

    def __disconnect__(self):
        """Close connection."""
        try:
            if self.conn is not None:
                self.conn.close()
                logging.info("DB disconnected")
        finally:
            self.conn = None

    # ----- Query helpers ------------------------------------------------------
    def fetch_all(self, query, params=None):
        """Return list of rows; no autocommit."""
        self.__connect__()
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, params or ())
                rows = cur.fetchall()
                return rows
        finally:
            self.__disconnect__()

    def fetch_one(self, query, params=None):
        """Return a single row or None."""
        self.__connect__()
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, params or ())
                row = cur.fetchone()
                return row
        finally:
            self.__disconnect__()

    def execute(self, query, params=None):
        """
        Execute write (INSERT/UPDATE/DELETE). Returns (rowcount, lastrowid).
        Commits on success, rolls back on failure.
        """
        self.__connect__()
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, params or ())
                self.conn.commit()
                return cur.rowcount, cur.lastrowid
        except Exception:
            self.conn.rollback()
            raise
        finally:
            self.__disconnect__()


# Instantiate a reusable factory (fresh connection per call)
def get_db():
    return DB(
        DB_HOST,
        DB_USER,
        DB_PASS,
        DB_NAME,
        ssl_ca=DB_SSL_CA if os.path.exists(DB_SSL_CA) else None,
    )


# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------
def as_json(row_or_rows):
    """Convert Decimals to float and datetimes to ISO strings for clean JSON."""

    def convert_value(v):
        if isinstance(v, Decimal):
            return float(v)
        if isinstance(v, datetime):
            # seconds precision; assume UTC
            return v.isoformat(sep=" ", timespec="seconds")
        return v

    if row_or_rows is None:
        return None
    if isinstance(row_or_rows, list):
        return [{k: convert_value(v) for k, v in r.items()} for r in row_or_rows]
    return {k: convert_value(v) for k, v in row_or_rows.items()}


def bad_request(message, code=400):
    return jsonify({"error": {"code": code, "message": message}}), code


def require_json():
    if not request.is_json:
        return None, bad_request("Content-Type must be application/json", 415)
    return request.get_json(silent=True) or {}, None

# -----------------------------------------------------------------------------
# Home
# -----------------------------------------------------------------------------
@app.get("/")
def home():
    message="Temperature Logger API"
    return jsonify({"message": message})


# -----------------------------------------------------------------------------
# Health
# -----------------------------------------------------------------------------
@app.get("/api/v1/health")
def health():
    # optional: check DB
    try:
        db = get_db()
        db.fetch_one("SELECT 1 as ok")
        return jsonify({"status": "ok"})
    except Exception:
        return jsonify({"status": "degraded"}), 503


# -----------------------------------------------------------------------------
# SENSORS
# -----------------------------------------------------------------------------
@app.get("/api/v1/sensors")
def list_sensors():
    db = get_db()
    rows = db.fetch_all(
        "SELECT sensor_id, mac_addr, device_id, device_location FROM sensors ORDER BY sensor_id ASC"
    )
    return jsonify(as_json(rows))


@app.get("/api/v1/sensors/<int:sensor_id>")
def get_sensor(sensor_id: int):
    db = get_db()
    row = db.fetch_one(
        "SELECT sensor_id, mac_addr, device_id, device_location FROM sensors WHERE sensor_id=%s",
        (sensor_id,),
    )
    if not row:
        return bad_request(f"Sensor {sensor_id} not found", 404)
    return jsonify(as_json(row))


@app.post("/api/v1/sensors")
def create_sensor():
    data, err = require_json()
    if err:
        return err
    required = ["sensor_id", "mac_addr", "device_id", "device_location"]
    missing = [k for k in required if k not in data]
    if missing:
        return bad_request(f"Missing fields: {', '.join(missing)}")

    try:
        db = get_db()
        q = """
            INSERT INTO sensors (sensor_id, mac_addr, device_id, device_location)
            VALUES (%s, %s, %s, %s)
        """
        params = (
            int(data["sensor_id"]),
            data["mac_addr"],
            data["device_id"],
            data["device_location"],
        )
        db.execute(q, params)
        # read back
        created = get_db().fetch_one(
            "SELECT sensor_id, mac_addr, device_id, device_location FROM sensors WHERE sensor_id=%s",
            (int(data["sensor_id"]),),
        )
        return jsonify({"message": "sensor created", "sensor": as_json(created)}), 201
    except pymysql.err.IntegrityError as e:
        return bad_request(f"Integrity error: {e.args[1]}")
    except Exception as e:
        logging.exception("Create sensor failed")
        return bad_request("Failed to create sensor")


@app.put("/api/v1/sensors/<int:sensor_id>")
def update_sensor(sensor_id: int):
    data, err = require_json()
    if err:
        return err

    fields = []
    params = []
    for k in ("mac_addr", "device_id", "device_location"):
        if k in data:
            fields.append(f"{k}=%s")
            params.append(data[k])

    if not fields:
        return bad_request("No updatable fields provided")

    params.append(sensor_id)

    try:
        db = get_db()
        q = f"UPDATE sensors SET {', '.join(fields)} WHERE sensor_id=%s"
        rc, _ = db.execute(q, tuple(params))
        if rc == 0:
            return bad_request(f"Sensor {sensor_id} not found", 404)
        updated = get_db().fetch_one(
            "SELECT sensor_id, mac_addr, device_id, device_location FROM sensors WHERE sensor_id=%s",
            (sensor_id,),
        )
        return jsonify({"message": "sensor updated", "sensor": as_json(updated)})
    except Exception:
        logging.exception("Update sensor failed")
        return bad_request("Failed to update sensor")


# -----------------------------------------------------------------------------
# TEMPERATURE LOGS
# -----------------------------------------------------------------------------
@app.post("/api/v1/temperatures")
def create_temperature():
    """
    JSON:
    {

      "sensor_id": 0,

      "temperature_f": 72.5,
      "humidity": 44.3,
      "pressure": 995.2,
      "read_time": "2025-09-24 13:05:00"  # optional; UTC recommended
    }
    """
    data, err = require_json()
    if err:
        return err

    required = ["sensor_id", "temperature_f", "humidity", "pressure"]
    missing = [k for k in required if k not in data]
    if missing:
        return bad_request(f"Missing fields: {', '.join(missing)}")

    # parse read_time (optional)
    read_time = data.get("read_time")
    if read_time:
        try:
            read_time = read_time.replace("T", " ")
            dt = datetime.fromisoformat(read_time)
        except Exception:
            return bad_request(
                "read_time must be ISO-like (e.g., '2025-09-24 13:05:00')"
            )
    else:
        dt = datetime.utcnow()

    try:
        db = get_db()
        q = """
            INSERT INTO temperature_log (read_time, sensor_id, temperature_f, humidity, pressure)
            VALUES (%s, %s, %s, %s, %s)
        """
        params = (
            dt,
            int(data["sensor_id"]),
            float(data["temperature_f"]),
            float(data["humidity"]),
            float(data["pressure"]),
        )
        _, last_id = db.execute(q, params)
        created = get_db().fetch_one(
            "SELECT * FROM temperature_log WHERE log_id=%s", (last_id,)
        )
        return jsonify({"message": "log created", "log": as_json(created)}), 201
    except pymysql.err.IntegrityError as e:
        # likely FK violation on sensor_id
        return bad_request(f"Integrity error: {e.args[1]}")
    except Exception:
        logging.exception("Create log failed")
        return bad_request("Failed to create log")


@app.get("/api/v1/temperatures")
def list_temperatures():
    try:
        limit = min(int(request.args.get("limit", 100)), 1000)
        offset = int(request.args.get("offset", 0))
    except ValueError:
        return bad_request("limit and offset must be integers")

    db = get_db()
    rows = db.fetch_all(
        "SELECT * FROM temperature_log ORDER BY log_id DESC LIMIT %s OFFSET %s",
        (limit, offset),
    )
    return jsonify(as_json(rows))


@app.get("/api/v1/temperatures/<int:log_id>")
def get_log(log_id: int):
    db = get_db()
    row = db.fetch_one("SELECT * FROM temperature_log WHERE log_id=%s", (log_id,))
    if not row:
        return bad_request(f"log {log_id} not found", 404)
    return jsonify(as_json(row))


@app.put("/api/v1/temperatures/<int:log_id>")
def update_log(log_id: int):
    data, err = require_json()
    if err:
        return err

    sets = []
    params = []

    if "read_time" in data:
        try:
            rt = data["read_time"].replace("T", " ")
            dt = datetime.fromisoformat(rt)
            sets.append("read_time=%s")
            params.append(dt)
        except Exception:
            return bad_request(
                "read_time must be ISO-like (e.g., '2025-09-24 13:05:00')"
            )

    if "sensor_id" in data:
        sets.append("sensor_id=%s")
        try:
            params.append(int(data["sensor_id"]))
        except ValueError:
            return bad_request("sensor_id must be an integer")

    if "temperature_f" in data:
        sets.append("temperature_f=%s")
        try:
            params.append(float(data["temperature_f"]))
        except ValueError:
            return bad_request("temperature_f must be a number")

    if "humidity" in data:
        sets.append("humidity=%s")
        try:
            params.append(float(data["humidity"]))
        except ValueError:
            return bad_request("humidity must be a number")

    if "pressure" in data:
        sets.append("pressure=%s")
        try:
            params.append(float(data["pressure"]))
        except ValueError:
            return bad_request("pressure must be a number")

    if not sets:
        return bad_request("No updatable fields provided")

    params.append(log_id)

    try:
        db = get_db()
        q = f"UPDATE temperature_log SET {', '.join(sets)} WHERE log_id=%s"
        rc, _ = db.execute(q, tuple(params))
        if rc == 0:
            return bad_request(f"log {log_id} not found", 404)
        updated = get_db().fetch_one(
            "SELECT * FROM temperature_log WHERE log_id=%s", (log_id,)
        )
        return jsonify({"message": "log updated", "log": as_json(updated)})
    except pymysql.err.IntegrityError as e:
        return bad_request(f"Integrity error: {e.args[1]}")
    except Exception:
        logging.exception("Update log failed")
        return bad_request("Failed to update log")


@app.delete("/api/v1/temperatures/<int:log_id>")
def delete_log(log_id: int):
    try:
        db = get_db()
        rc, _ = db.execute("DELETE FROM temperature_log WHERE log_id=%s", (log_id,))
        if rc == 0:
            return bad_request(f"log {log_id} not found", 404)
        return jsonify({"message": "log deleted", "log_id": log_id})
    except Exception:
        logging.exception("Delete log failed")
        return bad_request("Failed to delete log")


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5003, debug=False)
