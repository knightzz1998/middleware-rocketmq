package cn.knightzz.middlerware.base.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.List;

/**
 * @author 王天赐
 * @title: PullConsumer
 * @description: Pull方式消费者
 * @create: 2023-10-06 17:54
 */
@Slf4j
public class PullConsumer {

    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {

        // 创建消费者组对象
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("log-group3");
        // 设置nameserver地址
        consumer.setNamesrvAddr("192.168.100.129:9876");
        consumer.start();

        MessageQueue messageQueue = new MessageQueue();
        String topic = "log-topic";
        String tag = "order";
        messageQueue.setTopic(topic);
        // 从队列 id= 0 的队列 获取数据
        messageQueue.setQueueId(0);
        // 这里的 broker-a 是从 web ui 界面获取的
        messageQueue.setBrokerName("broker-a");

        long startOffset = 0;
        PullResult pullResult = consumer.pull(messageQueue, tag, startOffset, 25);

        if (pullResult.getPullStatus().equals(PullStatus.FOUND)) {
            List<MessageExt> msgFoundList = pullResult.getMsgFoundList();
            msgFoundList.forEach(messageExt -> {
                String msgId = messageExt.getMsgId();
                int queueId = messageExt.getQueueId();
                long queueOffset = messageExt.getQueueOffset();
                String keys = messageExt.getKeys();
                String message = new String(messageExt.getBody());
                log.info("msgId : {} , queueId : {}, queueOffset: {} , keys : {}, message : {} ",
                        msgId, queueId, queueOffset, keys, message);
            });

            // 更新消费位置
            consumer.updateConsumeOffset(messageQueue, pullResult.getNextBeginOffset());
        }
        consumer.shutdown();
    }
}
