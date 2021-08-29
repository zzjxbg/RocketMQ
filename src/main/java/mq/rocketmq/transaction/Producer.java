package mq.rocketmq.transaction;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.concurrent.TimeUnit;

public class Producer {
    public static void main(String[] args) throws InterruptedException, MQBrokerException, RemotingException, MQClientException {
        //1.发送半消息
        TransactionMQProducer producer = new TransactionMQProducer("group5");
        //2.指定Nameserver地址
        producer.setNamesrvAddr("192.168.109.130:9876;192.168.109.132:9876");

        //添加事务监听器
        producer.setTransactionListener(new TransactionListener() {
            /**
             * 在该方法中执行本地事务(对发送的半消息进行处理)
             * @param message
             * @param o
             * @return
             */
            @Override
            public LocalTransactionState executeLocalTransaction(Message message, Object o) {
                if(StringUtils.equals("Tag1",message.getTags())) {
                    return LocalTransactionState.COMMIT_MESSAGE;
                } else if(StringUtils.equals("Tag2",message.getTags())) {
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                } else if(StringUtils.equals("Tag3",message.getTags())) {
                    return LocalTransactionState.UNKNOW;
                }
                return LocalTransactionState.UNKNOW;
            }

            /**
             * 该方法是MQ进行消息事务状态回查(对不做处理的半消息进行检查处理)
             * @param messageExt
             * @return
             */
            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
                System.out.println("消息的Tag" + messageExt.getTags());
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });
        //3.启动producer
        producer.start();
        //4.创建消息对象,指定主题Topic、Tag和消息体
        String[] tags = {"Tag1","Tag2","Tag3"};
        for (int i = 0;i < 3;i++) {
            /**
             * 参数一：消息主题Topic
             * 参数二：消息Tag
             * 参数三：消息内容
             */
            Message msg = new Message("TransactionTopic",tags[i],("Hello World" + i).getBytes());
            //5.发送消息
            SendResult result = producer.sendMessageInTransaction(msg,null);
            //发送状态
            SendStatus status = result.getSendStatus();
            System.out.println("发送结果:"+result);
            //线程睡1秒
            TimeUnit.SECONDS.sleep(1);
        }
        //6.关闭生产者producer
//        producer.shutdown();
    }
}
