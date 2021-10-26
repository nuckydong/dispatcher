package com.ebike;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class EbikeMqttClient implements MqttCallbackExtended {

    private static Logger Log = LoggerFactory.getLogger(EbikeMqttClient.class);

    static Properties properties = Config.getConfig();




    static private final String host = properties.getProperty("host", "127.0.0.1");

    static private final int port = Integer.valueOf(properties.getProperty("port", "1883"));

    static private final String clientId = properties.getProperty("clientId", "JUST4TEST");

    static private final String topicSub = properties.getProperty("topicSub", "123/456");

    static private final String topicPub = properties.getProperty("topicPub", "789/10jk");

    static private final String userName = properties.getProperty("userName", "");

    static private final String password = properties.getProperty("password", "");


    //=========================================================

    static volatile MqttClient mqttClient = null;

    public MqttClient connect() {
        try {
            if (mqttClient == null) {
                mqttClient = new MqttClient("tcp://" + host + ":" + port, clientId, new MemoryPersistence());
                MqttConnectOptions options = new MqttConnectOptions();
                options.setUserName(userName);
                options.setPassword(password.toCharArray());
                options.setAutomaticReconnect(true);
                options.setCleanSession(true);
                options.setConnectionTimeout(30);
                options.setKeepAliveInterval(20);
                mqttClient.setCallback(this);
                mqttClient.connect(options);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return mqttClient;
    }

    @Override
    public void connectionLost(Throwable throwable) {
        // 失去链接
        Log.info("Connection lost , the client will be automatic reconnect");
    }

    @Override
    public void messageArrived(String topic, MqttMessage mqttMessage) {
        // 收到消息
        try {
            String msg = new String(mqttMessage.getPayload(), "UTF8");
            Log.info("Topic: {}: receive message {}", topic, msg);

            JSONObject source = JSON.parseObject(msg);

            JSONObject target = new JSONObject(new LinkedHashMap<>());

            target.put("ver", source.getIntValue("ver"));
            target.put("type", "ctl-cmd");
            target.put("mid", source.getIntValue("mid"));
            target.put("ts", source.getIntValue("ts"));

            JSONObject body_source = JSON.parseObject(source.getString("body"));
            JSONObject rtcm3_source = JSON.parseObject(body_source.getString("rtcm3"));

            Map<String, Object> body_target = new LinkedHashMap<>();
            Map<String, Object> rtcm3_broadcast = new LinkedHashMap<>();
            rtcm3_broadcast.put("seq", source.getIntValue("mid"));
            if (null == rtcm3_source) {
                Log.info("Ignore current message");
                return;
            }
            Object len_obj = rtcm3_source.get("length");
            Object data_obj = rtcm3_source.get("data");
            if (null == len_obj || null == data_obj) {
                Log.info("Ignore current message");
                return;
            }
            rtcm3_broadcast.put("length", Integer.valueOf(len_obj.toString()));
            rtcm3_broadcast.put("data", data_obj.toString());
            body_target.put("rtcm3.broadcast", rtcm3_broadcast);
            target.put("body", body_target);
            // 转发消息
            String[] temp = topicPub.split(",");
            for (String str : temp) {
                msg = JSON.toJSONString(target, true);
                Log.info("Push message , topic: {}, message: {}", str, msg);
                publish(mqttClient, str.trim(), msg);
            }
        } catch (Exception e) {
            Log.error(e.getMessage(), e);
        }

    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
        Log.info("DeliveryComplete---------" + iMqttDeliveryToken.isComplete());
    }


    @Override
    public void connectComplete(boolean b, String s) {
        if (b) {
            Log.info("Client reconnect , resubscribe topics");
            // 重连的 需要重新订阅
            subscribe(mqttClient, topicSub);
        }
    }


    /**
     * 发布，默认qos为1，非持久化
     *
     * @param topic       .
     * @param pushMessage .
     */
    public void publish(MqttClient client, String topic, String pushMessage) {
        publish(client, 2, false, topic, pushMessage);
    }

    /**
     * 发布主题和消息队列
     *
     * @param qos         .
     * @param retained    .
     * @param topic       .
     * @param pushMessage .
     */
    public void publish(MqttClient clinet, int qos, boolean retained, String topic, String pushMessage) {
        MqttMessage message = new MqttMessage();
        message.setQos(qos);
        message.setRetained(retained);
        message.setPayload(pushMessage.getBytes());
        MqttTopic mTopic = clinet.getTopic(topic);
        if (null == mTopic) {
            Log.info("Topic not exist");
        }
        MqttDeliveryToken token;
        try {
            token = mTopic.publish(message);
            token.waitForCompletion();
        } catch (MqttPersistenceException e) {
            e.printStackTrace();
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    /**
     * 共享订阅
     *
     * @param topic
     * @return
     */
    public void shareSubscribe(MqttClient client, String topic) {
        subscribe(client, "$queue/" + topic, 0);
    }

    /**
     * 订阅某个主题，qos默认为0
     *
     * @param topic .
     */
    public void subscribe(MqttClient client, String topic) {
        subscribe(client, topic, 2);
    }

    /**
     * 订阅某个主题
     *
     * @param topic .
     * @param qos   .
     */
    public void subscribe(MqttClient client, String topic, int qos) {
        try {
            client.subscribe(topic, qos);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Log.info("Dispatcher start ...  ");
        try {
            EbikeMqttClient instance = new EbikeMqttClient();
            MqttClient client = instance.connect();
            Log.info("Mqtt broker :{}", client.getServerURI());
            instance.subscribe(client, topicSub);
        } catch (Exception e) {
            Log.error("Dispatcher start failed");
            Log.error(e.getMessage(), e);
        }

        Log.info("Dispatcher start success");
    }

}
