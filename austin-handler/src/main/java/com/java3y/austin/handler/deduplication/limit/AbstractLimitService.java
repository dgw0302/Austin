package com.java3y.austin.handler.deduplication.limit;

import com.java3y.austin.common.domain.TaskInfo;
import com.java3y.austin.handler.deduplication.service.AbstractDeduplicationService;

import java.util.*;

/**
 * @author cao
 * @date 2022-04-20 12:00
 */
public abstract class AbstractLimitService implements LimitService {


    /**
     * 获取得到当前消息模板所有的去重Key
     *
     * @param taskInfo
     * @return
     */
    protected List<String> deduplicationAllKey(AbstractDeduplicationService service, TaskInfo taskInfo) {
        List<String> result = new ArrayList<>(taskInfo.getReceiver().size());
        for (String receiver : taskInfo.getReceiver()) {

            /**
             *    md5(templateId + receiver + content(送文案模型))
             *      相同的内容相同的模板短时间内发给同一个人
             */

            /**
             *  receiver(接收者) + templateId(模板id) + sendChannel(发送渠道)
             *  一天内一个用户只能收到某个渠道的消息 N 次
             */
            String key = deduplicationSingleKey(service, taskInfo, receiver);
            result.add(key);
        }
        return result;
    }


    protected String deduplicationSingleKey(AbstractDeduplicationService service, TaskInfo taskInfo, String receiver) {

        return service.deduplicationSingleKey(taskInfo, receiver);

    }



}
