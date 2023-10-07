package cn.knightzz.middlerware.base.producer;

import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.RandomUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * @author 王天赐
 * @title: SyncProducer
 * @description:
 * @create: 2023-10-06 15:10
 */
@Slf4j
public class SyncProducer {

    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {

        // 创建生产者, 并定义生产者组名称
        DefaultMQProducer producer = new DefaultMQProducer("log-group2");
        // 指定nameserver地址
        producer.setNamesrvAddr("192.168.100.129:9876");
        // 指定消息发送失败时的重试次数, 默认是2次
        producer.setRetryTimesWhenSendFailed(5);
        // 设置发送超时时间为 5s
        producer.setSendMsgTimeout(5000);

        // 开启生产者
        producer.start();

        // 生产并发送100条消息
        String topic = "log-topic";
        String tag = "order";
        for (int i = 0; i < 100; i++) {
            // 每条消息都会有对应的id
            String key = "key-" + i;
            String body = "INF : ==> log : 当前消息 " + i;
            Message message = new Message(topic, tag, key, body.getBytes());
            producer.send(message);
            log.info("发送消息 => key : {} , message : {}", key, body);
        }
        // 关闭生产者
        producer.shutdown();
    }

}
