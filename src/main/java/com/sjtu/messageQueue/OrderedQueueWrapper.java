package com.sjtu.messageQueue;

import com.aliyun.mns.client.CloudQueue;
import com.aliyun.mns.model.Message;
import com.sjtu.messageQueue.util.MQConstants;

import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by chenzhongpu on 5/16/16.
 */
public class OrderedQueueWrapper {

    private CloudQueue rawQueue;
    private MessageBuffer messageBuffer;
    private int sendSeqId;
    private int receSeqId;

    public OrderedQueueWrapper(CloudQueue queue, long msgBufferSize) {
        rawQueue = queue;
        messageBuffer = new MessageBuffer(msgBufferSize);
        sendSeqId = 1;
        receSeqId = 1;
    }

    public OrderedQueueWrapper(CloudQueue queue) {
        this(queue, MQConstants.BUFFER_SIZE);
    }

    private void addSeqIdToMessageBody(Message message, int seqId) {
        String idStr = String.format("#%d#", seqId);
        message.setMessageBody(idStr + message.getMessageBody());
    }

    private int parseSeqIdFromMessageBody(Message message) {
        if (message.getMessageBody().length() == 0) {
            System.err.println("message body is empty");
            return -1;
        }
        String body = message.getMessageBody();
        if (body.charAt(0) != '#') {
            System.err.println("message not start with seq id");
            return -1;
        }
        int pos = body.indexOf('#', 1);
        if (pos > 1) {
            String idStr = body.substring(1, pos);
            message.setMessageBody(body.substring(pos + 1));
            return Integer.valueOf(idStr);
        }
        return -1;
    }

    public void sendMessageInOrder(Message message) {
        addSeqIdToMessageBody(message, sendSeqId);
        rawQueue.putMessage(message);
        sendSeqId++;
    }

    public Message receiveMessageInOrder() {
        SeqMessage headMsg = messageBuffer.getMessage();
        while (headMsg == null) {
            Message receMsg = rawQueue.popMessage();
            int seqId = parseSeqIdFromMessageBody(receMsg);
            SeqMessage seqMessage = new SeqMessage(seqId, receMsg);
            rawQueue.deleteMessage(receMsg.getReceiptHandle());
            INSERT_RESPONSE response = messageBuffer.insertMessage(seqMessage);
            switch (response) {
                case INSERT_QUITE:
                    headMsg = messageBuffer.getMessage();
                    break;
                case RECEIVED_BEFORE:
                    headMsg = seqMessage;
                    break;
                case BUFFER_FULL:
                    return null;
            }
        }
        return headMsg.getMessage();
    }

    public void delete() {
        rawQueue.delete();
    }

}

class SeqMessage {

    private int seqId;
    private Message message;

    public SeqMessage(int seqId, Message message) {
        this.seqId = seqId;
        this.message = message;
    }

    public int getSeqId() {
        return seqId;
    }

    public Message getMessage() {
        return message;
    }
}

enum INSERT_RESPONSE {
    BUFFER_FULL, RECEIVED_BEFORE, INSERT_QUITE
}

class MessageBuffer {

    private long bufferSize;
    private int nextSeqId;

    private PriorityQueue<SeqMessage> bufferQueue;


    public MessageBuffer(long bufferSize) {
        this.bufferSize = bufferSize;
        bufferQueue = new PriorityQueue<>(MQConstants.QUEUE_CAPACITY,
                (SeqMessage a, SeqMessage b)-> (Integer.valueOf(a.getSeqId()).compareTo(b.getSeqId())));
        nextSeqId = 1;

    }

    public INSERT_RESPONSE insertMessage(SeqMessage seqMessage) {
        if (bufferSize <= bufferQueue.size()) {
            System.err.println("message buffer is full");
            return INSERT_RESPONSE.BUFFER_FULL;
        }
        if (seqMessage.getSeqId() < nextSeqId) {
            System.out.println("message has been received before");
            return INSERT_RESPONSE.RECEIVED_BEFORE;
        }

        bufferQueue.add(seqMessage);
        return INSERT_RESPONSE.INSERT_QUITE;
    }

    public SeqMessage getMessage() {
        if (bufferQueue.size() == 0)
            return null;
        SeqMessage headMsg = bufferQueue.peek();
        if (headMsg.getSeqId() == nextSeqId) {
            nextSeqId++;
            bufferQueue.poll();
            return headMsg;
        }
        return null;
    }

    public int getNextSeqId() {
        return nextSeqId;
    }

}
