package mq.rocketmq.base.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

/**
 * 消息接收者
 */
public class Consumer {
    public static void main(String[] args) throws MQClientException {
       // 1.创建消费者Consumer,制定消费者组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group1");
       // 2.指定Nameserver地址
        consumer.setNamesrvAddr("192.168.109.130:9876;192.168.109.132:9876");
       // 3.订阅主题Topic和Tag
        consumer.subscribe("base","Tag1");

        //设定消费模式: 负载均衡|广播模式(默认负载均衡)
        consumer.setMessageModel(MessageModel.BROADCASTING);

       // 4.设置回调函数,处理消息
       // MessageListenerConcurrently:并发消费
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for(MessageExt msg : msgs) {
                    System.out.println(new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
       // 5.启动消费者consumer
        consumer.start();
    }
}
