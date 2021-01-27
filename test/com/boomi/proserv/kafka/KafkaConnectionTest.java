package com.boomi.proserv.kafka;

import java.util.List;

class KafkaConnectionTest {

    public static void main(String[] args) {
        String serverHost 		= "boomi.antsoftware.org:9092";
        String enablePolling 	= "true";
        String maxIdle 			= "1";
        String maxConnection 	= "20";
        String topicName 		= "test-topic";
        String message 			= "<test>hellofromjava</test>";

        KafkaConnection.setLocalExecution(true);

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

        String groupIp			= "consumers";	//Otherwise: org.apache.kafka.common.errors.InvalidGroupIdException: The configured groupId is invalid
        int pollingTime 		= 60000;
        try {
            List<String> documents = KafkaConnection.getConnection(
                    serverHost,
                    Boolean.parseBoolean(enablePolling),
                    Integer.parseInt(maxIdle),
                    Integer.parseInt(maxConnection),
                    groupIp
            ).getDocuments(topicName, pollingTime);

            for (int i = 0; i < documents.size(); i++) {
                System.out.println("Index "+i+": "+documents.get(i));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}