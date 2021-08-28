package mq.rocketmq.base.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.concurrent.TimeUnit;

/**
 * 发送单向消息
 */
public class OneWayProducer {
    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        //1.创建消息生产者producer,并制定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        //2.指定Nameserver地址
        producer.setNamesrvAddr("192.168.109.130:9876;192.168.109.132:9876");
        //3.启动producer
        producer.start();
        //4.创建消息对象,指定主题Topic、Tag和消息体
        for (int i = 0;i < 3;i++) {
            /**
             * 参数一：消息主题Topic
             * 参数二：消息Tag
             * 参数三：消息内容
             */
            Message msg = new Message("base","Tag3",("Hello World,单向消息" + i).getBytes());
            //5.发送单向消息
            producer.sendOneway(msg);
            //线程睡1秒
            TimeUnit.SECONDS.sleep(5);
        }
        //6.关闭生产者producer
        producer.shutdown();
    }
}
