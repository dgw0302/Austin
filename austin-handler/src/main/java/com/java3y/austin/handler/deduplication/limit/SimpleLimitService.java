package com.java3y.austin.handler.deduplication.limit;

import cn.hutool.core.collection.CollUtil;
import com.java3y.austin.common.constant.AustinConstant;
import com.java3y.austin.common.domain.TaskInfo;
import com.java3y.austin.handler.deduplication.DeduplicationParam;
import com.java3y.austin.handler.deduplication.service.AbstractDeduplicationService;
import com.java3y.austin.support.utils.RedisUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 采用普通的计数去重方法，限制的是每天发送的条数。
 * 一天内N次相同渠道去重
 * 普通计数限流是当前时间到今天结束之间的时间进行限流，随着时间流逝，限流时间会减少
 *
 * @author cao
 * @date 2022-04-20 13:41
 */

/**
 * redis中去重key对应的value是一天内相同的用户如果已经收到某渠道内容n次
 *
 * 采用redisUtils.mGet批量操作。查出去重Key集合对应的value,根据对应的value与 param.getCountNum()（需达到的次数去重）比较。
 *
 * 如果达到去重条件
 *
 *
 *
 * 筛选符合条件的用户（筛选某个用户一天内收到相同渠道消息n次），redis里面，如果value != null,key对应的value还存在，说明还没有过期，但是还在设定的时间内。去重Key对应的value值大于  在一天内相同的用户如果已经收到某渠道内容n次，得去重
 *
 *
 *
 *  **筛选完得更新Redis**
 *
 *  如果redis对应的去重Key存在，value加一存进redis
 *
 *
 *
 *  如果不存在，直接存进redis,value值是1
 *
 *
 *
 *  设置 key-value 并设置过期时间
 *  限制到当天24点，过期时间设置的是时间戳
 *
 *
 */
@Service(value = "SimpleLimitService")
public class SimpleLimitService extends AbstractLimitService {

    private static final String LIMIT_TAG = "SP_";

    @Autowired
    private RedisUtils redisUtils;

    @Override
    public Set<String> limitFilter(AbstractDeduplicationService service, TaskInfo taskInfo, DeduplicationParam param) {

        //按照任务消息的接收人数构建一个set集合
        Set<String> filterReceiver = new HashSet<>(taskInfo.getReceiver().size());
        // 获取redis记录
        Map<String, String> readyPutRedisReceiver = new HashMap(taskInfo.getReceiver().size());
        //redis数据隔离
        /**
         * 获取得到当前消息模板所有的去重Key，并把得到的key加上LIMIT_TAG
         */
        List<String> keys = deduplicationAllKey(service, taskInfo).stream().map(key -> LIMIT_TAG + key).collect(Collectors.toList());
        //批量获取值，并将key和value组成map，value一天内相同的用户已经收到某渠道内容n次
        Map<String, String> inRedisValue = redisUtils.mGet(keys);

        for (String receiver : taskInfo.getReceiver()) {

            //生成去重Key,在下面比较
            String key = LIMIT_TAG + deduplicationSingleKey(service, taskInfo, receiver);
            String value = inRedisValue.get(key);

            // 筛选符合条件的用户，redis里面，如果value != null,key对应的value还存在，说明还没有过期，但是还在设定的时间内。去重Key对应的value值大于  在一天内相同的用户如果已经收到某渠道内容n次，得去重
            if (value != null && Integer.parseInt(value) >= param.getCountNum()) {
                //去重的话，先把这个用户加入filter集合中
                filterReceiver.add(receiver);
            } else {//如果没有达到设置的值，也就是说在某段时间内，一天内相同的用户已经收到某渠道内容次数小于设定值
                readyPutRedisReceiver.put(receiver, key);
            }
        }

        // 不符合条件的用户：需要更新Redis(无记录添加，有记录则累加次数)
        // param.getDeduplicationTime()是指某段时间的具体时间参数
        putInRedis(readyPutRedisReceiver, inRedisValue, param.getDeduplicationTime());

        //返回重复的用户集合
        return filterReceiver;
    }
    /**
     * 存入redis 实现去重
     *            //如果这个去重key之前存在但是没有达到某天收到同一渠道的次数
     * @param readyPutRedisReceiver
     */
        private void putInRedis(Map<String, String> readyPutRedisReceiver,
                            Map<String, String> inRedisValue, Long deduplicationTime) {

        //key是receiver，value是去重key
        Map<String, String> keyValues = new HashMap<>(readyPutRedisReceiver.size());


        for (Map.Entry<String, String> entry : readyPutRedisReceiver.entrySet()) {
            String key = entry.getValue();
            if (inRedisValue.get(key) != null) {

                //如果redis对应的去重Key存在，value加一存进redis
                keyValues.put(key, String.valueOf(Integer.valueOf(inRedisValue.get(key)) + 1));
            } else {
                //如果不存在，直接存进redis,value值是1
                keyValues.put(key, String.valueOf(AustinConstant.TRUE));
            }
        }
        if (CollUtil.isNotEmpty(keyValues)) {
            //设置 key-value 并设置过期时间
            //在某段时间内同一用户不能收到相同的消息，的某段时间就是靠redis的key设置过期时间
            //下面这个是批量操作
            //限制到当天24点，过期时间设置的是时间戳
            redisUtils.pipelineSetEx(keyValues, deduplicationTime);
        }
    }

}
