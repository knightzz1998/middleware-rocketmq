package cn.knightzz.middlerware.base.producer;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * @author 王天赐
 * @title: OnewayProducer
 * @description: 单项发送消息
 * @create: 2023-10-07 18:09
 */
public class OnewayProducer {

    public static void main(String[] args) {

        DefaultMQProducer producer = new DefaultMQProducer("pg");
        producer.setNamesrvAddr("192.168.100.129:9876");
        try {
            producer.start();
            for (int i = 0; i < 10; i++) {
                byte[] body = ("Hi," + i).getBytes();
                Message msg = new Message("single", "someTag", body);
                // 单向发送
                producer.sendOneway(msg);
            }
        } catch (MQClientException | RemotingException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        producer.shutdown();
        System.out.println("producer shutdown");

    }
}
