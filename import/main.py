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
SET t.hash = $hash, t.timestamp = datetime($timestamp), t.totalBTC = toFloat($total_amount), t.totalUSD = toFloat($total_usd),
    t.flowBTC = toFloat($flow_btc), t.flowUSD = toFloat($flow_usd)
FOREACH (f in $from_address | MERGE (a:Address {id:f.address}) CREATE (a)-[:SENT {valueBTC:toFloat(f.value), valueUSD: toFloat(f.value_usd)}]->(t))
FOREACH (to in $to_address | MERGE (a:Address {id:to.address}) CREATE (t)-[:SENT {valueBTC: toFloat(to.value), valueUSD: toFloat(to.value_usd)}]->(a))
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


def test_neo4j_connection():
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
    wait(test_neo4j_connection, timeout_seconds=300, sleep_seconds=5)


def on_message(ws, message):
    """
    Function gets executed for every new message it receives from the websocket
    """
    global session, x, bp

    # Hearbeat every 1000 transactions
    if x % 1000 == 0:
        print(f"Imported {x} transactions")
    x += 1

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

    # Define from addresses and their values
    from_address = list()
    # Addresses to be ignored while calculating the actual flow of value
    ignore_address = list()

    for row in message["x"]["inputs"]:
        if not row['prev_out']['addr']:
            continue
        ignore_address.append(row["prev_out"]["addr"])
        from_address.append({'address': row["prev_out"]["addr"], 'value': int(
            row["prev_out"]["value"]) / SATOSHI_TO_BITCOIN, 'value_usd': int(
            row["prev_out"]["value"]) / SATOSHI_TO_BITCOIN * btc_to_usd})

    # Define to address and their values
    to_address = list()
    for row in message["x"]["out"]:
        if not row['addr']:
            continue
        to_address.append(
            {'address': row['addr'], 'value': int(row['value']) / SATOSHI_TO_BITCOIN, 'value_usd': int(row['value']) / SATOSHI_TO_BITCOIN * btc_to_usd})

    # Define how much value has actually been transfered
    # Ignore outgoing transactions that have the same addresses as incoming addresses, which return value to the original sender

    flow_btc = sum([int(output["value"]) /
                    SATOSHI_TO_BITCOIN for output in message["x"]["out"] if not output['addr'] in ignore_address])
    flow_usd = flow_btc * btc_to_usd

    # Define Cypher params
    params = {'hash': hash, 'timestamp': timestamp, 'total_amount': total_amount, 'total_usd': total_usd,
              'from_address': from_address, 'to_address': to_address, 'flow_btc': flow_btc, 'flow_usd': flow_usd}
    # Import data
    try:
        session.run(import_query, params)
    except Exception as e:
        print(f"Import to Neo4j failed due to {e}")


def on_error(ws, error):
    print(f"Websocket error:{error}")
    ws.close()
    connect_websocket()


def on_open(ws):
    print("Websocket connection opened")
    # Subscribing to Unconfirmed transactions
    ws.send('{"op":"unconfirmed_sub"}')


def connect_websocket():
    ws = websocket.WebSocketApp(
        "wss://ws.blockchain.info/inv",
        on_message=on_message,
        on_error=on_error,
        on_open=on_open
    )

    ws.run_forever()


if __name__ == '__main__':

    bp = BitcoinPrice()

    driver = GraphDatabase.driver(
        NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))

    # Wait until connection to neo4j has been established
    print("Testing Neo4j connection")
    wait_valid_connection()
    print("Connection to Neo4j established")

    with driver.session() as session:
        connect_websocket()
