package com.pushtechnology.field;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;

import java.util.List;
import java.util.Properties;

public class DiffusionChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<String, String>> {

    private enum TableType {
        ROW,
        ARRAY,
        OBJECT
    };

    private final DiffusionWrapper diffusionWrapper;

    private final ObjectMapper mapper = new ObjectMapper();
    private final Properties properties;

    private final TableIndexCache tableIndexCache = new TableIndexCache();

    public DiffusionChangeConsumer(DiffusionWrapper diffusionWrapper, Properties properties) {
        this.diffusionWrapper = diffusionWrapper;
        this.properties = properties;
    }

    @Override
    public void handleBatch(List<ChangeEvent<String, String>> records, DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> committer) throws InterruptedException {

        for(ChangeEvent<String, String> r : records) {
            if(r.value() == null) {
                return;
            }

            // Parse JSON
            try {
                JsonNode jsonNode = mapper.readTree(r.value());

                JsonNode payloadNode = jsonNode.get("payload");
                if(payloadNode != null) {
                    JsonNode opNode = payloadNode.get("op");
                    if(opNode == null) {
                        continue;
                    }

                    JsonNode sourceNode = payloadNode.get("source");
                    String db = sourceNode.get("db").textValue();
                    String table = sourceNode.get("table").textValue();

                    JsonNode before = payloadNode.get("before");
                    JsonNode after = payloadNode.get("after");

                    String operation = opNode.textValue().toLowerCase();
                    switch(operation) {
                        case "c":
                            addOrUpdateRow(db, table, after);
                            break;
                        case "u":
                            addOrUpdateRow(db, table, after);
                            break;
                        case "d":
                            deleteRow(db, table, before);
                            break;
                        default:
                            System.out.println("Unknown operation '" + operation + "'");
                            break;
                    }
                }

            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }
    }

    private String getRowName(String db, String table, JsonNode node) {
        String propertyKey = "table." + table + ".key";
        String keyRowName = properties.getProperty(propertyKey);

        if(keyRowName == null) {
            System.out.println("No key specified for table " + table);
            return null;
        }

        String rowName = node.get(keyRowName).asText();
        return rowName;
    }

    private TableType getTableType(String table) {
        switch(properties.getProperty("table." + table + ".type", "row").toLowerCase()) {
            case "array":
                return TableType.ARRAY;
            case "object":
                return TableType.OBJECT;
        }
        return TableType.ROW;
    }

    private void addOrUpdateRow(String db, String table, JsonNode after) {
        String rowName = getRowName(db, table, after);

        if(rowName != null) {
            TableType tableType = getTableType(table);

            String topicName;
            switch(tableType) {
                case ARRAY:
                    topicName = db + "/" + table;

                    if (!tableIndexCache.hasTable(table)) {
                        diffusionWrapper.addArrayTopic(topicName);
                    }

                    int index = tableIndexCache.getIndex(table, rowName);
                    if (index == -1) {
                        tableIndexCache.addIndex(table, rowName);
                        diffusionWrapper.patchTopicAddArray(topicName, after.toString());
                    }
                    else {
                        diffusionWrapper.patchTopicReplaceArray(topicName, after.toString(), index);
                    }
                    break;
                case OBJECT:
                    topicName = db + "/" + table;
                    if(!tableIndexCache.hasTable(table)) {
                        diffusionWrapper.addObjectTopic(topicName);
                    }

                    if(tableIndexCache.getIndex(table, rowName) == -1) {
                        tableIndexCache.addIndex(table, rowName);
                        diffusionWrapper.patchTopicAddObject(topicName, rowName, after.toString());
                    }
                    else {
                        diffusionWrapper.patchTopicReplaceObject(topicName, rowName, after.toString());
                    }
                    break;
                default:
                    topicName = db + "/" + table + "/" + rowName;
                    diffusionWrapper.updateTopic(topicName, after.toString());
                    break;
            }
        }
    }

    private void deleteRow(String db, String table, JsonNode before) {
        String rowName = getRowName(db, table, before);
        if (rowName != null) {
            TableType tableType = getTableType(table);

            String topicName;
            switch(tableType) {
                case ARRAY:
                    topicName = db + "/" + table;
                    int index = tableIndexCache.removeIndex(table, rowName);
                    if(index == -1) {
                        return; // Don't know about this row
                    }
                    diffusionWrapper.patchTopicRemoveArray(topicName, index);
                    break;
                case OBJECT:
                    topicName = db + "/" + table;
                    if(tableIndexCache.removeIndex(table, rowName) == -1) {
                        return; // Don't know about this row
                    }
                    diffusionWrapper.patchTopicRemoveObject(topicName, rowName);
                    break;
                default:
                    topicName = db + "/" + table + "/" + rowName;
                    diffusionWrapper.deleteTopic(topicName);
            }
        }
    }
}
