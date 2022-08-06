package com.java3y.austin.handler.receiver.kafka;

import cn.hutool.core.collection.CollUtil;
import com.alibaba.fastjson.JSON;
import com.java3y.austin.common.domain.AnchorInfo;
import com.java3y.austin.common.domain.LogParam;
import com.java3y.austin.common.domain.TaskInfo;
import com.java3y.austin.common.enums.AnchorState;
import com.java3y.austin.handler.handler.HandlerHolder;
import com.java3y.austin.handler.pending.Task;
import com.java3y.austin.handler.pending.TaskPendingHolder;
import com.java3y.austin.handler.receiver.service.ConsumeService;
import com.java3y.austin.handler.utils.GroupIdMappingUtils;
import com.java3y.austin.support.constans.MessageQueuePipeline;
import com.java3y.austin.support.domain.MessageTemplate;
import com.java3y.austin.support.utils.LogUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

/**
 * @author 3y
 * 消费MQ的消息
 */
@Slf4j
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@ConditionalOnProperty(name = "austin-mq-pipeline", havingValue = MessageQueuePipeline.KAFKA)
public class Receiver {
    @Autowired
    private ConsumeService consumeService;
    /**
     * 发送消息
     *
     *
     * 对于有@KafkaListener注解的方法，可以消费消息
     *
     * @param consumerRecord
     * @param topicGroupId
     */
    @KafkaListener(topics = "#{'${austin.business.topic.name}'}", containerFactory = "filterContainerFactory")
    public void consumer(ConsumerRecord<?, String> consumerRecord, @Header(KafkaHeaders.GROUP_ID) String topicGroupId) {
        //从topic拿到单条消息
        Optional<String> kafkaMessage = Optional.ofNullable(consumerRecord.value());
        if (kafkaMessage.isPresent()) {
            //拿到消息内部的SendTaskModel（发送消息任务模型）的taskInfo属性（发送任务的信息），可能有多个，也就是发送多条消息
            //发送的taskInfoLists包含多个任务TaskInfo，可能包含不同渠道不同消息类型
            List<TaskInfo> taskInfoLists = JSON.parseArray(kafkaMessage.get(), TaskInfo.class);
            //拿到消息内部的第一个（反正每条消息的多个taskInofo的groupid是一样的）
            String messageGroupId = GroupIdMappingUtils.getGroupIdByTaskInfo(CollUtil.getFirst(taskInfoLists.iterator()));
            /**
             * 每个消费者组 只消费 他们自身关心的消息
             * 一共18个消费者组
             */
            if (topicGroupId.equals(messageGroupId)) {
                consumeService.consume2Send(taskInfoLists);
            }
        }
    }

    /**
     * 撤回消息
     * @param consumerRecord
     */
    @KafkaListener(topics = "#{'${austin.business.recall.topic.name}'}",groupId = "#{'${austin.business.recall.group.name}'}",containerFactory = "filterContainerFactory")
    public void recall(ConsumerRecord<?,String> consumerRecord){
        Optional<String> kafkaMessage = Optional.ofNullable(consumerRecord.value());
        if(kafkaMessage.isPresent()){
            MessageTemplate messageTemplate = JSON.parseObject(kafkaMessage.get(), MessageTemplate.class);
            consumeService.consume2recall(messageTemplate);
        }
    }
}
