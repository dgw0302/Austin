package com.java3y.austin.handler.deduplication;

import com.java3y.austin.common.constant.AustinConstant;
import com.java3y.austin.common.domain.TaskInfo;
import com.java3y.austin.common.enums.DeduplicationType;
import com.java3y.austin.support.service.ConfigService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author 3y.
 * @date 2021/12/12
 * 去重服务
 */
@Service
public class DeduplicationRuleService {

    public static final String DEDUPLICATION_RULE_KEY = "deduplicationRule";

    @Autowired
    private ConfigService config;

    @Autowired
    private DeduplicationHolder deduplicationHolder;

    public void duplication(TaskInfo taskInfo) {
        // 配置样例：{"deduplication_10":{"num":1,"time":300},"deduplication_20":{"num":5}}
        String deduplicationConfig = config.getProperty(DEDUPLICATION_RULE_KEY, AustinConstant.APOLLO_DEFAULT_VALUE_JSON_OBJECT);

        // 去重
        //获得去重类型，目前只有两种，10：N分钟相同内容去重，20：一天内N次相同渠道去重；
        List<Integer> deduplicationList = DeduplicationType.getDeduplicationList();
        //开始枚举去重的类型,10和20
        for (Integer deduplicationType : deduplicationList) {
            //根据每种去重类型获得去重服务所需要的参数
            DeduplicationParam deduplicationParam = deduplicationHolder.selectBuilder(deduplicationType).build(deduplicationConfig, taskInfo);
            if (deduplicationParam != null) {
                //根据去重所需要的参数，获得对应的接口并执行接口里的方法
                deduplicationHolder.selectService(deduplicationType).deduplication(deduplicationParam);
            }
        }
    }


}
