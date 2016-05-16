package com.sjtu.messageQueue;

import com.aliyun.mns.client.CloudAccount;
import com.aliyun.mns.client.CloudQueue;
import com.aliyun.mns.client.MNSClient;
import com.aliyun.mns.common.utils.ServiceSettings;
import com.aliyun.mns.model.Message;
import com.aliyun.mns.model.QueueMeta;
import com.sjtu.messageQueue.util.MQConstants;

/**
 * Created by chenzhongpu on 5/16/16.
 */
public class MQSender {

    public static void main(String[] args) {

        CloudAccount account = new CloudAccount(ServiceSettings.getMNSAccessKeyId(),
                ServiceSettings.getMNSAccessKeySecret(),
                ServiceSettings.getMNSAccountEndpoint());

        MNSClient client = account.getMNSClient();
        CloudQueue queue = client.getQueueRef(MQConstants.QUEUE_NAME);
        QueueMeta queueMeta = new QueueMeta();
        queueMeta.setPollingWaitSeconds(MQConstants.POLLING_WAIT);
        queueMeta.setMaxMessageSize(MQConstants.MAX_MSG_SIZE);

        try {
            queue.create(queueMeta);
            System.out.println("Success to create queue");

//            for (int i = 1; i < 20; i ++) {
//                Message message = new Message();
//                message.setMessageBody("i am message " + i);
//                queue.putMessage(message);
//            }

            OrderedQueueWrapper orderedMQ = new OrderedQueueWrapper(queue);
            for (int i = 1; i < 20; i++) {
                Message message = new Message();
                message.setMessageBody("i am message " + i);
                orderedMQ.sendMessageInOrder(message);
            }

            client.close();

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Fail to create queue");
            System.exit(1);
        }

    }
}
