package com.java3y.austin.handler.deduplication.limit;

import cn.hutool.core.util.IdUtil;
import com.java3y.austin.common.domain.TaskInfo;
import com.java3y.austin.handler.deduplication.DeduplicationParam;
import com.java3y.austin.handler.deduplication.service.AbstractDeduplicationService;
import com.java3y.austin.support.utils.RedisUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * 滑动窗口去重器（内容去重采用基于redis中zset的滑动窗口去重，可以做到严格控制单位时间内的频次。）
 *滑动窗口是用lua脚本实现的，每次调用去重方法都会重新计算滑动窗口的起始时间
 * @author cao
 * @date 2022-04-20 11:34
 */
@Service(value = "SlideWindowLimitService")
public class SlideWindowLimitService extends AbstractLimitService {

    private static final String LIMIT_TAG = "SW_";

    @Autowired
    private RedisUtils redisUtils;


    private DefaultRedisScript<Long> redisScript;


    @PostConstruct
    public void init() {
        redisScript = new DefaultRedisScript();
        redisScript.setResultType(Long.class);
        redisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("limit.lua")));
    }


    /**
     * @param service  去重器对象
     * @param taskInfo
     * @param param    去重参数
     * @return 返回不符合条件的手机号码
     */
    @Override
    public Set<String> limitFilter(AbstractDeduplicationService service, TaskInfo taskInfo, DeduplicationParam param) {

        Set<String> filterReceiver = new HashSet<>(taskInfo.getReceiver().size());
        long nowTime = System.currentTimeMillis();
        /**
         *
         *
         *    再回顾一下去重要求，要求相同用户在n分钟内不能收到相同的消息n次。这个去重key是由md5(templateId + receiver + content(送文案模型))组成
         *
         *    每个去重key的value都是一个zset结构，调用lua脚本
         *    删除当前key对应的value（zset），value是雪花算法生成的唯一值，valuesocre是当前时间戳
         *    Lua脚本先根据根据当前时间和滑动窗口减去zset里面过期的value。
         *    然后查去重key对应的value有多少个
         *    如果查出来的value大于等于去重设定值那么当前taskinfo对应的receiver就应该被舍弃
         *     如果查出来的value等于null或者小于去重设定值，那么就放进redis.
         *
         *
         *
         * ：
         */

        for (String receiver : taskInfo.getReceiver()) {
            //为每个用户生成去重key
            String key = LIMIT_TAG + deduplicationSingleKey(service, taskInfo, receiver);
            //雪花算法生成zsetd的唯一value.
            String scoreValue = String.valueOf(IdUtil.getSnowflake().nextId());
            //value的valuescore是当前时间戳.
            String score = String.valueOf(nowTime);
            //redisScript是加载的lua脚本
            //为什么key要转成list，我暂时没查出来，可能是lua脚本这样规定，但是每次Lua脚本只操作keys[1]一个key
            if (redisUtils.execLimitLua(redisScript, Arrays.asList(key), String.valueOf(param.getDeduplicationTime() * 1000), score, String.valueOf(param.getCountNum()), scoreValue)) {
                filterReceiver.add(receiver);
            }

        }
        return filterReceiver;
    }


}
