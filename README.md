# db-adapter
Debezium-based database source adapter for Diffusion.

Push Technology do not provide any support for this project. It is provided as an example only.
## Introduction

This Diffusion adapter can be used to replicate database tables as Diffusion topics. It uses change data capture (CDC) via the Debezium library to monitor changes to the tables, and reflect those changes into the topic model. Tables are mapped to JSON topics in one of three ways.

- **Object** : A table is mapped to a JSON topic, with each record being a JSON object keyed by the table's primary key.

- **Array** : A table is mapped to a JSON topic, with all records as entries in an array.
 
- **Row** : All records are mapped as individual JSON child topics below a topic path representing the table.

Note: Replication is one-way only; updates to the topic model are not propagated back to the database.

## Building

Debezium has connectors for different databases; the default configuration for the project is to use MySQL. This can be changed by editing `pom.xml` and changing `debezium-connector-mysql` as appropriate.

To build the adapter, run:
```
mvn clean package
```

The adapter jar file will be written to the `target` directory.

## Running

The jar file may be run directly, reading its configuration in from a properties file. If the name of this properties file is not specified, it assumes a default of `adapter.properties`.

```
java -jar db-adapter-<version>.jar [properties_file]
```

## Configuration

The configuration file contains a series of key/value pairs that fall into three categories:

### Debezium/database parameters

These are parameters that the Debezium engine requires for connecting to the database.

#### Example

```
connector.class=io.debezium.connector.mysql.MySqlConnector
name=diffusion
offset.storage=org.apache.kafka.connect.storage.MemoryOffsetBackingStore
offset.storage.file.filename=/tmp/offsets.dat
offset.flush.interval.ms=60000
tasks.max=1
schemas.enable=false

database.server.name=diffusion
database.server.id=8192
database.history=MemoryDatabaseHistory
database.history.file.filename=dbhistory.dat

database.hostname=localhost
database.port=3306
database.user=root
database.password=password
database.include=grocery
```

If not specified, defaults for the following values are the same as listed above:

- connector.class
- name
- offset.storage
- offset.flush.interval
- database.server.name
- database.server.id
- database.history
- database.history.file.filename

### Diffusion connection parameters

```
diffusion.host=localhost
diffusion.port=8090
diffusion.username=control
diffusion.password=password
```

These parameters default to the values given above, if not specified.

### Table/topic mapping

Each table to be replicated must be specified in the configuration file by giving the primary key; this is required for tracking the records being changed and associating them with either a Diffusion topic or a record within a topic.

The following property tells the adapter to replicate the table "`items`" which has the primary key "`id`"
```
table.items.key=id
```

The format of the JSON topic that is produced is controlled by the "`type`" parameter for the table, e.g.
```
table.items.type=array
table.items.type=object
table.items.type=row
```

Consider the following table "`stock`" in the database "`grocery`":

|     item | quantity | reorder_quantity |
|---------:|---------:|-----------------:|
|   Apples |       80 |               40 |
| Cabbages |      100 |               50 |
| Tomatoes |       50 |               25 |


### object
```
Topic: grocery/stock
{
    "Apples" : {
        "item" : "Apples",
        "quantity" : "80",
        "reorder_quantity" : "40"
    },
    "Cabbages" : {
        "item" : "Cabbages",
        "quantity" : "100",
        "reorder_quantity" : "50"
    },
    "Tomatoes" : {
        "item" : "Tomatoes",
        "quantity" : "50",
        "reorder_quantity" : "25"
    }
}

```

### array
```
Topic: grocery/stock
[
    [ 
        "item" : "Apples",
        "quantity" : "80",
        "reorder_quantity" : "40"
    ],
    [
        "item" : "Cabbages",
        "quantity" : "100",
        "reorder_quantity" : "50"
    ],
    [
        "item" : "Tomatoes",
        "quantity" : "50",
        "reorder_quantity" : "25"
    ]

]
```


### row

```
Topic: grocery/stock/Apples
{
    "item" : "Apples",
    "quantity" : "80",
    "reorder_quantity" : "40"
}

Topic: grocery/stock/Cabbages
{
    "item" : "Cabbages",
    "quantity" : "100",
    "reorder_quantity" : "50"
}

Topic: grocery/stock/Tomatoes
{
    "item" : "Tomatoes",
    "quantity" : "50",
    "reorder_quantity" : "25"
}
```
