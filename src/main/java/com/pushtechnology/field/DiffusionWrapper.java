package com.pushtechnology.field;

import com.pushtechnology.diffusion.client.Diffusion;
import com.pushtechnology.diffusion.client.features.TopicUpdate;
import com.pushtechnology.diffusion.client.features.control.topics.TopicControl;
import com.pushtechnology.diffusion.client.session.Session;
import com.pushtechnology.diffusion.client.session.SessionEstablishmentException;
import com.pushtechnology.diffusion.client.topics.details.TopicSpecification;
import com.pushtechnology.diffusion.client.topics.details.TopicType;
import com.pushtechnology.diffusion.datatype.json.JSON;

import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DiffusionWrapper {

    private final Properties properties;
    private final TopicSpecification jsonSpec = Diffusion.newTopicSpecification(TopicType.JSON);

    private Session session;

    private Lock lock = new ReentrantLock();

    public DiffusionWrapper(Properties properties) {
        this.properties = properties;
    }

    private Session simpleConnectionAttempt() {
        if (session != null && !session.getState().isClosed()) {
            return session;
        }

        try {
            session = Diffusion.sessions()
                    .inputBufferSize(1024)
                    .serverHost(properties.getProperty("diffusion.host", "localhost"))
                    .serverPort(Integer.parseInt(properties.getProperty("diffusion.port", "8090")))
                    .principal(properties.getProperty("diffusion.username", "control"))
                    .password(properties.getProperty("diffusion.password", "password"))
                    .listener(new Session.Listener.Default() {
                        @Override
                        public void onSessionStateChanged(Session session, Session.State oldState, Session.State newState) {
                            System.out.println("Session state changed from " + oldState + " to " + newState);
                        }
                    })
                    .open();
        } catch (SessionEstablishmentException ex) {
            System.err.println("Unable to connect to Diffusion, retrying");
        }

        return session;
    }

    public void connect() {
        while(session == null || session.getState().isClosed()) {
            session = simpleConnectionAttempt();
            if(session == null) {
                try {
                    Thread.sleep(5000);
                }
                catch(InterruptedException ignore) {
                }
                continue;
            }
        }
    }

    public void updateTopic(String topic, String value) {
        JSON json = Diffusion.dataTypes().json().fromJsonString(value);
        session.feature(TopicUpdate.class).addAndSet(topic, jsonSpec, JSON.class, json);
    }

    public void deleteTopic(String topic) {
        session.feature(TopicControl.class).removeTopics(">" + topic);
    }

    public void addArrayTopic(String topic) {
        JSON emptyJson = Diffusion.dataTypes().json().fromJsonString("[]");
        session.feature(TopicUpdate.class).addAndSet(topic, jsonSpec, JSON.class, emptyJson);
    }

    public void addObjectTopic(String topic) {
        JSON emptyJson = Diffusion.dataTypes().json().fromJsonString("{}");
        session.feature(TopicUpdate.class).addAndSet(topic, jsonSpec, JSON.class, emptyJson);
    }

    public void patchTopicAddArray(String topic, String value) {
        String patch = "[{\"op\":\"add\",\"path\":\"/-\", \"value\":" + value + "}]";
        session.feature(TopicUpdate.class).applyJsonPatch(topic, patch);
    }

    public void patchTopicReplaceArray(String topic, String value, int index) {
        String patch = "[{\"op\":\"replace\",\"path\":\"/" + index + "\", \"value\":" + value + "}]";
        session.feature(TopicUpdate.class).applyJsonPatch(topic, patch);
    }

    public void patchTopicRemoveArray(String topic, int index) {
        String patch = "[{\"op\":\"remove\",\"path\":\"/" + index + "\"}]";
        session.feature(TopicUpdate.class).applyJsonPatch(topic, patch);
    }

    public void patchTopicAddObject(String topic, String key, String value) {
        String patch = "[{\"op\":\"add\",\"path\":\"/" + key + "\",\"value\":" + value + "}]";
        session.feature(TopicUpdate.class).applyJsonPatch(topic, patch);
    }

    public void patchTopicReplaceObject(String topic, String key, String value) {
        String patch = "[{\"op\":\"replace\",\"path\":\"/" + key + "\",\"value\":" + value + "}]";
        session.feature(TopicUpdate.class).applyJsonPatch(topic, patch);
    }

    public void patchTopicRemoveObject(String topic, String key) {
        String patch = "[{\"op\":\"remove\",\"path\":\"/" + key + "\"}]";
        System.out.println("Delete object: " + patch);
        session.feature(TopicUpdate.class).applyJsonPatch(topic, patch);
    }

    public void close() {
        if(session != null && ! session.getState().isClosed()) {
            session.close();
        }
    }
}
