# bitcoin-to-neo4jdash
Project that listens to bitcoin websocket API for new transactions and stores them to Neo4j to be analyzed

[](img/btc.png)


## Getting started

First define the constraints and indexes in Neo4j by running

```
sh define_schema.sh
```

Now you can instantiate the whole project by running

```
docker-compose up -d
```

Once the project is up and running, open NeoDash at `localhost:80` in your browser.

Click on the New Dashboard option.

[](img/neodash1.png)

Next, connect to the Neo4j instance.

Username: neo4j

Password: letmein

Host: localhost

Once you have successfuly connected, click on the load dashboard option on the left toolbar

[](img/neodash2.png)

Next, select the `dashboard.json` file which is available in the `neodash` folder and click _Load Dashboard_.

[](img/neodash3.png)

## Graph model

[](img/graphmodel.png)

The graph consists of addresses and transactions. The transaction nodes contain the transaction hash and the timestamp and some preprocessed information like the total and the flow value of the transaction. The original input and output contributions are stored as relationship properties to allow multiple inputs and outputs with various contributions. The direction of the relationships indicates the flow of value.