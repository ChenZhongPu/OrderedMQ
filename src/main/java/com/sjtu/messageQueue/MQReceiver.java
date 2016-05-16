package com.sjtu.messageQueue;

import com.aliyun.mns.client.CloudAccount;
import com.aliyun.mns.client.CloudQueue;
import com.aliyun.mns.client.MNSClient;
import com.aliyun.mns.common.utils.ServiceSettings;
import com.aliyun.mns.model.Message;
import com.sjtu.messageQueue.util.MQConstants;

/**
 * Created by chenzhongpu on 5/16/16.
 */
public class MQReceiver {

    public static void main(String[] args) {

        CloudAccount account = new CloudAccount(ServiceSettings.getMNSAccessKeyId(),
                ServiceSettings.getMNSAccessKeySecret(),
                ServiceSettings.getMNSAccountEndpoint());

        MNSClient client = account.getMNSClient();
        CloudQueue queue = client.getQueueRef(MQConstants.QUEUE_NAME);

//        for (int i = 1; i < 20; i++) {
//            Message popMsg = queue.popMessage();
//            if (popMsg != null) {
//                System.out.println(popMsg.getMessageBody());
//                queue.deleteMessage(popMsg.getReceiptHandle());
//            }
//        }

        OrderedQueueWrapper orderedMQ = new OrderedQueueWrapper(queue);
        for (int i = 1; i < 20; i++) {
            Message popMsg = orderedMQ.receiveMessageInOrder();
            if (popMsg != null) {
                System.out.println(popMsg.getMessageBody());
            }
        }

        //queue.delete();
        orderedMQ.delete();
        client.close();
     }
}
