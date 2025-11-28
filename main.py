#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import configparser
import json
import logging
import os
import sqlite3
import sys
import time
from collections import OrderedDict
from typing import Dict, Any, Tuple, Optional

import minimalmodbus
import serial
import paho.mqtt.client as mqtt
import ssl

from log_util import setup_logger


# -----------------------------------------------------------
# 設定檔與常數
# -----------------------------------------------------------

BASE_NAME = os.path.splitext(os.path.basename(__file__))[0]
CONFIG_FILE = f"{BASE_NAME}.ini"
DB_PATH = "spm1_queue.db"

cfg = configparser.ConfigParser()
cfg.read(CONFIG_FILE, encoding="utf-8")

# RS485 設定（對應 [RS485]）
RS485_PORT = cfg.get("RS485", "PORT")
RS485_BAUD = cfg.getint("RS485", "BAUDRATE")
RS485_BYTESIZE = cfg.getint("RS485", "BYTESIZE")
RS485_STOPBITS = cfg.getint("RS485", "STOPBITS")
RS485_PARITY_S = cfg.get("RS485", "PARITY").upper()
RS485_TIMEOUT = cfg.getfloat("RS485", "TIMEOUT")

# 場站與裝置編號（對應 [PLACE]）
PLACE = cfg.get("PLACE", "PLACE", fallback="UNKNOWN")
DEVICE_NUMBER_STR = cfg.get("PLACE", "DEVICE_NUMBER", fallback="1")
device_ids = [int(x.strip()) for x in DEVICE_NUMBER_STR.split(",") if x.strip()]

# 其他設定（對應 [ELSE]）
SLEEP_TIME = cfg.getfloat("ELSE", "SLEEP_TIME", fallback=60.0)

# MQTT 設定（對應 [MQTT]）
MQTT_USER = cfg.get("MQTT", "ACCOUNT", fallback="")
MQTT_PASS = cfg.get("MQTT", "PASSWORD", fallback="")
MQTT_HOST = cfg.get("MQTT", "HOST")
MQTT_PORT = cfg.getint("MQTT", "PORT")
MQTT_TOPIC = cfg.get("MQTT", "TOPIC")
MQTT_KEEPALIVE = 60  # ini 沒寫，給一個固定值

# logger
logger = setup_logger(
    name="spm1_mqtt",
    folder_name="logs",
    base_filename="spm1_data",
    level=logging.INFO,
)

logger.info("Loaded config from %s", CONFIG_FILE)

# -----------------------------------------------------------
# RS485 / Modbus – SPM-1 暫存器定義
# -----------------------------------------------------------

BYTEORDER_32F = minimalmodbus.BYTEORDER_LITTLE_SWAP  # 32-bit float, little-endian swap

# 暫存器位址（依照 bytecode 內容）
AC_V_A = 0x1000
AC_V_B = 0x1002
AC_V_C = 0x1004
AC_I_A = 0x1010
AC_I_B = 0x1012
AC_I_C = 0x1014
AC_FREQ = 0x1018
AC_KW_P_TOTAL = 0x1020
AC_PF = 0x1038
AC_KWH = 0x103A

MODULE9 = OrderedDict(
    [
        ("ac_v_a", {"addr": AC_V_A, "functioncode": 4, "round": 1}),
        ("ac_v_b", {"addr": AC_V_B, "functioncode": 4, "round": 1}),
        ("ac_v_c", {"addr": AC_V_C, "functioncode": 4, "round": 1}),
    ]
)

MODULE10 = OrderedDict(
    [
        ("ac_i_a", {"addr": AC_I_A, "functioncode": 4, "round": 3}),
        ("ac_i_b", {"addr": AC_I_B, "functioncode": 4, "round": 3}),
        ("ac_i_c", {"addr": AC_I_C, "functioncode": 4, "round": 3}),
        ("ac_freq", {"addr": AC_FREQ, "functioncode": 4, "round": 2}),
    ]
)

MODULE11 = OrderedDict(
    [
        ("ac_kw_p_total", {"addr": AC_KW_P_TOTAL, "functioncode": 4, "round": 3}),
        ("ac_pf", {"addr": AC_PF, "functioncode": 4, "round": 3}),
        ("ac_kwh", {"addr": AC_KWH, "functioncode": 4, "round": 3}),
    ]
)

DEVICE_META: "OrderedDict[str, Dict[str, Any]]" = OrderedDict(
    [
        ("M9", MODULE9),
        ("M10", MODULE10),
        ("M11", MODULE11),
    ]
)

# -----------------------------------------------------------
# SQLite queue – 離線緩衝 MQTT 訊息
# -----------------------------------------------------------

conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cur = conn.cursor()

cur.execute(
    """
CREATE TABLE IF NOT EXISTS queue (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    topic   TEXT NOT NULL,
    payload TEXT NOT NULL,
    ts      TEXT NOT NULL,
    sent    INTEGER NOT NULL DEFAULT 0
)
"""
)

cur.execute(
    """
CREATE INDEX IF NOT EXISTS idx_queue_sent_id ON queue(sent, id)
"""
)

conn.commit()


def now_string() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def enqueue_to_db(topic: str, payload: str, ts: str) -> None:
    try:
        cur.execute(
            "INSERT INTO queue (topic, payload, ts, sent) VALUES (?, ?, ?, 0)",
            (topic, payload, ts),
        )
        conn.commit()
        logger.debug("Enqueue to DB ok (topic=%s ts=%s)", topic, ts)
    except Exception as e:
        logger.error("Enqueue to DB failed: %s", e)


def flush_db_queue(client: Optional[mqtt.Client], max_batch: int) -> int:
    if client is None:
        return 0

    try:
        cur.execute(
            "SELECT id, topic, payload FROM queue WHERE sent=0 "
            "ORDER BY id LIMIT ?",
            (max_batch,),
        )
        rows = cur.fetchall()
    except Exception as e:
        logger.error("Read DB queue failed: %s", e)
        return 0

    if not rows:
        return 0

    logger.info("Start flushing DB queue, count=%d ...", len(rows))
    sent_count = 0

    for row_id, topic, payload in rows:
        try:
            info = client.publish(topic, payload, qos=1, retain=False)
            info.wait_for_publish()
            if info.is_published():
                cur.execute("UPDATE queue SET sent=1 WHERE id=?", (row_id,))
                conn.commit()
                sent_count += 1
            else:
                logger.warning(
                    "Flush DB queue publish failed (id=%s): not published", row_id
                )
        except Exception as e:
            logger.error("Flush DB queue publish failed (id=%s): %s", row_id, e)
            break

    # 清掉太舊的 sent=1 資料（只保留最新大約 5000 筆）
    try:
        cur.execute(
            """
            DELETE FROM queue
            WHERE sent=1 AND id < (
                SELECT IFNULL(MAX(id) - 5000, 0) FROM queue
            )
            """
        )
        deleted = cur.rowcount
        conn.commit()
        if deleted:
            logger.info("Cleanup DB queue, deleted %d old sent rows", deleted)
    except Exception as e:
        logger.error("Cleanup DB queue failed: %s", e)

    return sent_count


# -----------------------------------------------------------
# RS485 讀取
# -----------------------------------------------------------

def read_all_data(
    instrument: minimalmodbus.Instrument,
    dev_id: int,
    device_meta: "OrderedDict[str, Dict[str, Any]]",
) -> Tuple[Dict[str, float], bool]:
    instrument.address = dev_id

    data: Dict[str, float] = {}
    valid_flags = []

    for module_name, tbl in device_meta.items():
        for name, meta in tbl.items():
            addr = meta["addr"]
            functioncode = meta.get("functioncode", 4)
            ndigits = meta.get("round", 3)

            try:
                value = instrument.read_float(
                    addr,
                    functioncode=functioncode,
                    number_of_registers=2,
                    byteorder=BYTEORDER_32F,
                )
                rounded_value = round(value, ndigits)
                data[name] = rounded_value
                valid_flags.append(True)
            except Exception as err:
                valid_flags.append(False)
                logger.warning(
                    "485 Read Error - dev %s %s@0x%04X: %s",
                    dev_id,
                    module_name,
                    addr,
                    err,
                )

    if any(valid_flags):
        logger.debug("Valid 485 data: %s", data)
        return data, True
    else:
        logger.warning("No valid 485 data")
        return {}, False


# -----------------------------------------------------------
# payload 組裝
# -----------------------------------------------------------

def make_payload(measures: Dict[str, float], place: str, device_id: int) -> str:
    ts = now_string()

    body = OrderedDict()
    body["timestamp"] = ts
    body["place"] = place
    body["device_id"] = str(device_id)
    body["measures"] = measures

    payload = json.dumps(
        body,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return payload


# -----------------------------------------------------------
# MQTT 連線
# -----------------------------------------------------------

def connect_mqtt() -> Optional[mqtt.Client]:
    client = None
    try:
        client = mqtt.Client()
        if MQTT_USER:
            client.username_pw_set(MQTT_USER, MQTT_PASS)

        # 原 bytecode 有使用 TLS
        client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
        client.tls_insecure_set(False)

        client.connect(MQTT_HOST, MQTT_PORT, MQTT_KEEPALIVE)
        client.loop_start()
        logger.info("MQTT connected to %s:%s", MQTT_HOST, MQTT_PORT)
        return client
    except Exception as e:
        logger.error("MQTT connect failed: %s", e)
        if client is not None:
            try:
                client.loop_stop()
                client.disconnect()
            except Exception:
                pass
        return None


def close_mqtt(client: Optional[mqtt.Client]) -> None:
    if client is None:
        return
    try:
        client.loop_stop()
        client.disconnect()
    except Exception as e:
        logger.error("Error while closing MQTT: %s", e)


# -----------------------------------------------------------
# 主程式
# -----------------------------------------------------------

def main() -> None:
    logger.info(
        "Start polling SPM-1 devices %s every %ss and publishing to MQTT Topic '%s'...",
        device_ids,
        SLEEP_TIME,
        MQTT_TOPIC,
    )

    instrument = minimalmodbus.Instrument(RS485_PORT, device_ids[0])
    instrument.serial.baudrate = RS485_BAUD
    instrument.serial.bytesize = RS485_BYTESIZE
    instrument.serial.stopbits = RS485_STOPBITS

    parity_map = {
        "NONE": serial.PARITY_NONE,
        "EVEN": serial.PARITY_EVEN,
        "ODD": serial.PARITY_ODD,
    }
    instrument.serial.parity = parity_map.get(RS485_PARITY_S, serial.PARITY_NONE)
    instrument.serial.timeout = RS485_TIMEOUT
    instrument.mode = minimalmodbus.MODE_RTU

    mqtt_client = connect_mqtt()
    max_batch = 100  # 一次 flush 最多多少筆 queue

    try:
        while True:
            logger.info("Processing %d devices: %s", len(device_ids), device_ids)

            for dev_id in device_ids:
                measures, has_valid = read_all_data(instrument, dev_id, DEVICE_META)

                if has_valid:
                    logger.info("Device %s 485 data: %s", dev_id, measures)
                else:
                    logger.info("Device %s no valid data received", dev_id)

                payload = make_payload(measures, PLACE, dev_id)
                logger.info("[Topic] %s | [Payload] %s", MQTT_TOPIC, payload)

                ts = now_string()
                enqueue_to_db(MQTT_TOPIC, payload, ts)

                if mqtt_client is None:
                    logger.warning("MQTT client is None, try reconnect...")
                    mqtt_client = connect_mqtt()

                if mqtt_client is not None:
                    try:
                        info = mqtt_client.publish(
                            MQTT_TOPIC,
                            payload,
                            qos=1,
                            retain=False,
                        )
                        info.wait_for_publish()
                    except Exception as e:
                        logger.error("Flush DB queue error: %s", e)
                else:
                    logger.warning(
                        "MQTT not connected, data will stay in local queue."
                    )

            try:
                flush_db_queue(mqtt_client, max_batch)
            except Exception as e:
                logger.error("Flush DB queue error: %s", e)

            logger.info("Cycle completed, sleeping for %s seconds...", SLEEP_TIME)
            time.sleep(SLEEP_TIME)

    except KeyboardInterrupt:
        logger.info("Stopping...")
    except Exception as e:
        logger.error("Unexpected error: %s", e)
    finally:
        try:
            conn.close()
            logger.info("SQLite DB connection closed")
        except Exception:
            pass

        try:
            close_mqtt(mqtt_client)
            logger.info("MQTT client disconnected")
        except Exception:
            pass


if __name__ == "__main__":
    if not device_ids:
        logger.error("DEVICE_NUMBER 在 .ini 中未設定或為空，程式結束。")
        sys.exit(1)

    main()
