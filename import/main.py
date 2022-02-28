import time
import websocket
import json
import os
import time
import requests
from waiting import wait
from neo4j import GraphDatabase

NEO4J_URI = os.environ.get('NEO4J_URI', 'neo4j://neo4j:7687')
NEO4J_USER = os.environ.get('NEO4J_USER', 'neo4j')
NEO4J_PASS = os.environ.get('NEO4J_PASS', 'letmein')

SATOSHI_TO_BITCOIN = 100000000

import_query = """
CREATE (t:Transaction)
SET t.hash = $hash, t.timestamp = datetime($timestamp), t.totalAmount = toFloat($total_amount), t.totalUSD = toFloat($total_usd)
FOREACH (f in $from_address | MERGE (a:Address {id:f.address}) CREATE (a)-[:SENT {value:toFloat(f.value), valueUSD: toFloat(f.value_usd)}]->(t))
FOREACH (to in $to_address | MERGE (a:Address {id:to.address}) CREATE (t)-[:SENT {value: toFloat(to.value), valueUSD: toFloat(to.value_usd)}]->(a))
"""

x = 0


class BitcoinPrice():
    def __init__(self):
        self.url = "https://blockchain.info/ticker"
        self.update_price()

    def update_price(self):
        try:
            data = requests.get(self.url).json()
            priceUSD = data["USD"]["15m"]
            self.priceUSD = priceUSD
            self.last_updated = time.time()
            print(f"Bitcoin price updated at {self.priceUSD}")
        except Exception as e:
            print(f"Failed fetching latest bitcoin price")

    def get_price(self):
        if time.time() - self.last_updated < (60 * 60):
            return self.priceUSD
        else:
            self.update_price()
            return self.priceUSD


def test_connection():
    global driver
    with driver.session() as session:
        try:
            session.run("""
            RETURN true
            """)
            return True
        except:
            return False


def wait_valid_connection():
    """
    Function that waits for Neo4j connection to be valid
    """
    print("Testing neo4j connection")
    wait(test_connection, timeout_seconds=300, sleep_seconds=5)


def on_message(ws, message):
    global session, x, bp

    message = json.loads(message)

    # Get current bitcoin price
    btc_to_usd = bp.get_price()

    # Data preparation
    hash = message["x"]["hash"]
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%S",
                              time.localtime(message["x"]["time"]))
    total_amount = sum([int(output["value"]) /
                        SATOSHI_TO_BITCOIN for output in message["x"]["out"]])
    total_usd = total_amount * btc_to_usd

    # Define from addresses
    from_address = list()
    for row in message["x"]["inputs"]:
        if not row['prev_out']['addr']:
            continue
        from_address.append({'address': row["prev_out"]["addr"], 'value': int(
            row["prev_out"]["value"]) / SATOSHI_TO_BITCOIN, 'value_usd': int(
            row["prev_out"]["value"]) / SATOSHI_TO_BITCOIN * btc_to_usd})

    # Define to address
    to_address = list()
    for row in message["x"]["out"]:
        if not row['addr']:
            continue
        to_address.append(
            {'address': row['addr'], 'value': int(row['value']) / SATOSHI_TO_BITCOIN, 'value_usd': int(row['value']) / SATOSHI_TO_BITCOIN * btc_to_usd})

    # Define Cypher params
    params = {'hash': hash, 'timestamp': timestamp, 'total_amount': total_amount, 'total_usd': total_usd,
              'from_address': from_address, 'to_address': to_address}
    # Import data
    try:
        session.run(import_query, params)
    except Exception as e:
        print(e)

    # Ping every 1000 transactions
    x += 1
    if x % 1000 == 0:
        print(f"Imported {x} transactions already")


def on_error(ws, error):
    print(error)


def on_open(ws):
    print("Websocket connection opened")
    ws.send('{"op":"unconfirmed_sub"}')


if __name__ == '__main__':

    bp = BitcoinPrice()

    driver = GraphDatabase.driver(
        NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))

    # Wait until connection to neo4j has been established
    wait_valid_connection()

    print("Connection to Neo4j established")
    with driver.session() as session:
        ws = websocket.WebSocketApp(
            "wss://ws.blockchain.info/inv",
            on_message=on_message,
            on_error=on_error,
            on_open=on_open
        )

        ws.run_forever()
