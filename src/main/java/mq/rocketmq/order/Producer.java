package mq.rocketmq.order;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class Producer {
    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {

        //1.创建消息生产者producer,并制定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        //2.指定Nameserver地址
        producer.setNamesrvAddr("192.168.109.130:9876;192.168.109.132:9876");
        //3.启动producer
        producer.start();

        //构建消息集合
        List<OrderStep> orderSteps = OrderStep.buildOrders();
        //发送消息
        for(int i = 0;i < orderSteps.size();i++) {
            String body = orderSteps.get(i) + "";
            Message msg = new Message("OrderTopic", "Order", "i"+i, body.getBytes());
            /**
             * 参数一:消息对象
             * 参数二:消息队列的选择器
             * 参数三:选择队列的业务标识(订单ID)
             */
            SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                /**
                 *
                 * @param list: 队列集合
                 * @param message: 消息对象
                 * @param o: 业务标识的参数
                 * @return
                 */
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    // 订单ID相同的进入同一队列(保证分区有序)
                    long orderId = (long)o;
                    long index = orderId % list.size();
                    return list.get((int)index);
                }
            },orderSteps.get(i).getOrderId());
            System.out.println("发送结果: " + sendResult);
        }
        producer.shutdown();
    }
}
