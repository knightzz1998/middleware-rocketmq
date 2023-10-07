package cn.knightzz.middlerware.base.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.Random;

/**
 * @author 王天赐
 * @title: PushConsumer
 * @description:
 * @create: 2023-10-06 18:17
 */
@Slf4j
public class PushConsumer {

    public static void process01() throws MQClientException {
        handlerMessage("log-group","log-topic", "order");
    }

    public static void handlerMessage(String group, String topic, String tag) {
        // 创建 push 类型的消费者对象 , 并设置消费者组
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
        consumer.setNamesrvAddr("192.168.100.129:9876");
        try {
            // 订阅一个或多个topic，并指定tag过滤条件，这里指定*表示接收所有tag的消息
            consumer.subscribe(topic, tag);
            // 注册回调接口来处理从Broker中收到的消息
            consumer.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                                ConsumeConcurrentlyContext context) {
                    msgs.forEach(messageExt -> {
                        log.info("当前线程 {} , 消息ID {} , 接收到消息 : {}", Thread.currentThread().getName(), messageExt.getMsgId(),
                                new String(messageExt.getBody()));
                    });
                    // 返回消费状态
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            consumer.start();

        } catch (MQClientException e) {
            throw new RuntimeException(e);
        }finally {
            consumer.shutdown();
        }
    }

    public static void main(String[] args) throws MQClientException {
        //handlerMessage("log-group","log-topic", "order");
        handlerMessage("order-group","order-topic", "book");
    }

    static class Task implements Runnable {

        @Override
        public void run() {

            // 创建 push 类型的消费者对象 , 并设置消费者组
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("log-group");
            consumer.setNamesrvAddr("192.168.100.129:9876");
            String topic = "log-topic";
            String tag = "order";
            // 订阅一个或多个topic，并指定tag过滤条件，这里指定*表示接收所有tag的消息
            try {
                consumer.subscribe(topic, tag);
                // 注册回调接口来处理从Broker中收到的消息
                consumer.registerMessageListener(new MessageListenerConcurrently() {
                    @Override
                    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                                    ConsumeConcurrentlyContext context) {
                        msgs.forEach(messageExt -> {
                            log.info("当前线程 {} , 消息ID {} , 接收到消息 : {}", Thread.currentThread().getName(), messageExt.getMsgId(),
                                    new String(messageExt.getBody()));

                        });
                        // 返回消费状态
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                });
                consumer.start();
            } catch (MQClientException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
