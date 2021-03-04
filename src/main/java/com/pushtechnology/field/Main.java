package com.pushtechnology.field;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.relational.history.MemoryDatabaseHistory;
import io.debezium.util.Clock;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
    private static final String SERVER_NAME = "diffusion";

    private final Properties properties;
    private final Properties userProperties;

    private DiffusionWrapper diffusionWrapper;
    private DiffusionChangeConsumer consumer;

    public Main(String propertiesFile) throws IOException {
        this.properties = new Properties();
        this.userProperties = new Properties();
        this.userProperties.load(new FileReader(propertiesFile));
    }

    public void run() {
        properties.put("connector.class", "io.debezium.connector.mysql.MySqlConnector");
        properties.put("name", SERVER_NAME);
        properties.put("offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore");
        properties.put("offset.storage.file.filename", "/tmp/offsets.dat");
        properties.put("offset.flush.interval.ms", 60000);

        properties.put("database.server.name", SERVER_NAME);
        properties.put("database.server.id", 8192);
        properties.put("database.history", MemoryDatabaseHistory.class.getName());
        properties.put("database.history.file.filename", "dbhistory.dat");

        for(String key : userProperties.stringPropertyNames()) {
            properties.put(key, userProperties.getProperty(key));
        }

        diffusionWrapper = new DiffusionWrapper(properties);
        diffusionWrapper.connect();

        consumer = new DiffusionChangeConsumer(diffusionWrapper, properties);

        DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
                .using(properties)
                .notifying(consumer)
                .build();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down");
            diffusionWrapper.close();
        }));

        awaitTermination(executor);

        executor.shutdown();
    }

    private void awaitTermination(ExecutorService executor) {
        try {
            while(!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                System.out.println("Waiting for Debezium engine to complete");
            }
        }
        catch(InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        System.out.println("Terminated");
    }

    public static void main(String[] args) throws Exception {
        String filename = "adapter.properties";
        if(args.length > 0) {
            filename = args[0];
        }

        Main app = new Main(filename);
        app.run();
    }
}
