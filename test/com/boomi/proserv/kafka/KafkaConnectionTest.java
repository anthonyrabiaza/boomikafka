package com.boomi.proserv.kafka;

import java.util.Date;
import java.util.List;

class KafkaConnectionTest {

    public static void main(String[] args) {
        String serverHost 		= "boomi.antsoftware.org:9092";
        String enablePolling 	= "true";
        String maxIdle 			= "1";
        String maxConnection 	= "20";
        String topicName 		= "test-topic";
        String message 			= "<test>Hello from java sent at " + new Date()+ "</test>";

        KafkaConnection.setLocalExecution(true);

        System.out.println("Sending message...");
        try {
            KafkaConnection.getConnection(
                    serverHost,
                    Boolean.parseBoolean(enablePolling),
                    Integer.parseInt(maxIdle),
                    Integer.parseInt(maxConnection)
            ).sendDocuments(topicName, message);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Message sent");

        System.out.println("Sleeping for 5 sec...");
        try {
            Thread.sleep(5 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        String groupIp			= "consumers";	//Otherwise: org.apache.kafka.common.errors.InvalidGroupIdException: The configured groupId is invalid
        int pollingTime 		= 60;

        System.out.println("Polling topic...");
        try {
            List<String> documents = KafkaConnection.getConnection(
                    serverHost,
                    Boolean.parseBoolean(enablePolling),
                    Integer.parseInt(maxIdle),
                    Integer.parseInt(maxConnection),
                    groupIp
            ).getDocuments(topicName, pollingTime, false);
            System.out.println("Polling done");
            if(documents.size()==0) {
                System.out.println("No document returned");
            } else {
                for (int i = 0; i < documents.size(); i++) {
                    System.out.println("Index " + i + ": " + documents.get(i));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}