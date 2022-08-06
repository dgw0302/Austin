package com.java3y.austin.handler.pending;


import cn.hutool.core.collection.CollUtil;
import com.java3y.austin.common.domain.TaskInfo;
import com.java3y.austin.handler.deduplication.DeduplicationRuleService;
import com.java3y.austin.handler.discard.DiscardMessageService;
import com.java3y.austin.handler.handler.HandlerHolder;
import com.java3y.austin.handler.shield.ShieldService;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

/**
 * Task 执行器
 * 0.丢弃消息
 * 2.屏蔽消息
 * 2.通用去重功能
 * 3.发送消息
 *
 * @author 3y
 */
@Data
@Accessors(chain = true)
@Slf4j
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class Task implements Runnable {
    //channel->Handler的映射关系
    @Autowired
    private HandlerHolder handlerHolder;

    @Autowired
    private DeduplicationRuleService deduplicationRuleService;

    @Autowired
    private DiscardMessageService discardMessageService;

    @Autowired
    private ShieldService shieldService;

    private TaskInfo taskInfo;


    @Override
    public void run() {

        // 0. 丢弃消息
        if (discardMessageService.isDiscard(taskInfo)) {
            return;
        }
        // 1. 屏蔽消息
        shieldService.shield(taskInfo);

        // 2.平台通用去重
        /**
         *       1.普通计数限流：是当前时间到今天结束之间的时间进行限流，随着时间流逝，限流时间会减少
         *
         *   redis中去重key对应的value是一天内相同的用户如果已经收到某渠道内容n次
         *  采用redisUtils.mGet批量操作。查出去重Key集合对应的value,根据对应的value与 param.getCountNum()（需达到的次数去重）比较。
         *
         * 如果达到去重条件
         * 筛选符合条件的用户（筛选某个用户一天内收到相同渠道消息n次），redis里面，如果value != null,key对应的value还存在，说明还没有过期，但是还在设定的时间内。去重Key对应的value值大于  在一天内相同的用户如果已经收到某渠道内容n次，得去重
         *
         * 筛选完得更新Redis**
         *
         * 如果redis对应的去重Key存在，value加一存进redis
         *
         * 如果不存在，直接存进redis,value值是1
         * 设置 key-value 并设置过期时间
         * 限制到当天24点，过期时间设置的是时间戳
         *
         *
         *        2.redis滑动窗口限流去重
         *
         *
         *
         *      滑动窗口是用lua脚本实现的，每次调用去重方法都会重新计算滑动窗口的起始时间
         *
         *    lua
         * *   再回顾一下去重要求，要求相同用户在n分钟内不能收到相同的消息n次。这个去重key是由md5(templateId + receiver + content(送文案模型))组成
         * *
         * *    每个去重key的value都是一个zset结构，调用lua脚本
         * *    删除当前key对应的value（zset），value是雪花算法生成的唯一值，valuesocre是当前时间戳
         * *    Lua脚本先根据根据当前时间和滑动窗口减去zset里面过期的value。
         * *    然后查去重key对应的value有多少个
         * *    如果查出来的value大于等于去重设定值那么当前taskinfo对应的receiver就应该被舍弃
         * *    如果查出来的value等于null或者小于去重设定值，那么就放进redis.
         *
         *
         *
         *
         *         采用的数据结构也不同 滑动窗口使用的zset，普通计数用的是string
         *
         *
         */
        if (CollUtil.isNotEmpty(taskInfo.getReceiver())) {
            deduplicationRuleService.duplication(taskInfo);
        }

        // 3. 真正发送消息
        if (CollUtil.isNotEmpty(taskInfo.getReceiver())) {
            //先路由到对应的handler然后执行doHandler方法
            handlerHolder.route(taskInfo.getSendChannel()).doHandler(taskInfo);
        }

    }
}
