package cn.knightzz.middlerware.base.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.concurrent.TimeUnit;

/**
 * @author 王天赐
 * @title: AsyncProducer
 * @description: 异步发送消息
 * @create: 2023-10-07 16:49
 */
@Slf4j
public class AsyncProducer {

    public static void main(String[] args) throws RemotingException, InterruptedException, MQClientException {

        DefaultMQProducer producer = new DefaultMQProducer("order-group");
        // 设置nameserver
        producer.setNamesrvAddr("192.168.100.129:9876");
        // 设置发送失败后不再重试
        producer.setRetryTimesWhenSendFailed(0);
        // 指定新创建的Topic的Queue数量为2，默认为4
        producer.setDefaultTopicQueueNums(2);
        producer.start();

        // 设置默认队列的数量, 默认是4, 假设是100条数据, 每个队列就是25条, offset 就是 0~24
        for (int i = 0; i < 100; i++) {
            String body = "book : " + i;
            Message message = new Message("order-topic", "book", body.getBytes());
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    SendStatus sendStatus = sendResult.getSendStatus();
                    int queueId = sendResult.getMessageQueue().getQueueId();
                    long queueOffset = sendResult.getQueueOffset();
                    log.info("status : {}, queueId : {} , offset : {} ", sendStatus, queueId, queueOffset);
                }

                @Override
                public void onException(Throwable e) {
                    e.printStackTrace();
                }
            });
        }

        // sleep一会儿
        // 由于采用的是异步发送，所以若这里不sleep，
        // 则消息还未发送就会将producer给关闭，报错
        TimeUnit.SECONDS.sleep(3);
        producer.shutdown();

    }
}
