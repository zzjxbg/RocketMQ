package mq.rocketmq.base.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.concurrent.TimeUnit;

/**
 * 发送异步消息
 */
public class AsyncProducer {
    public static void main(String[] args) throws MQClientException, InterruptedException, MQBrokerException, RemotingException {

        //1.创建消息生产者producer,并制定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        //2.指定Nameserver地址
        producer.setNamesrvAddr("192.168.109.130:9876;192.168.109.132:9876");
        //3.启动producer
        producer.start();
        //4.创建消息对象,指定主题Topic、Tag和消息体
        for (int i = 0;i < 10;i++) {
            /**
             * 参数一：消息主题Topic
             * 参数二：消息Tag
             * 参数三：消息内容
             */
            Message msg = new Message("base","Tag2",("Hello World" + i).getBytes());
            //5.发送异步消息
            producer.send(msg, new SendCallback() {
                /**
                 * 发送成功回调函数
                 * @param sendResult
                 */
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.println("发送结果:"+sendResult);
                }
                /**
                 * 发送失败回调函数
                 * @param e
                 */
                @Override
                public void onException(Throwable e) {
                    System.out.println("发送异常:"+e);
                }
            });
            //线程睡1秒
            TimeUnit.SECONDS.sleep(1);
        }
        //6.关闭生产者producer
        producer.shutdown();
    }
}
