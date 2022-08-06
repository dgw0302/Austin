package com.java3y.austin.handler.deduplication.build;

import com.java3y.austin.common.domain.TaskInfo;
import com.java3y.austin.common.enums.AnchorState;
import com.java3y.austin.common.enums.DeduplicationType;
import com.java3y.austin.handler.deduplication.DeduplicationParam;
import org.springframework.stereotype.Service;


/**
 * @author huskey
 * @date 2022/1/18
 * 根据配置构建去重参数
 * N分钟相同内容去重
 */
@Service
public class ContentDeduplicationBuilder extends AbstractDeduplicationBuilder implements Builder {

    public ContentDeduplicationBuilder() {
        deduplicationType = DeduplicationType.CONTENT.getCode();
    }

    @Override
    public DeduplicationParam build(String deduplication, TaskInfo taskInfo) {
        //DeduplicationParam：去重服务所需要的参数：TaskIno信息，去重时间，需达到的次数去重，标识属于哪种去重(数据埋点)
        DeduplicationParam deduplicationParam = getParamsFromConfig(deduplicationType, deduplication, taskInfo);
        if (deduplicationParam == null) {
           return null;
        }
        deduplicationParam.setAnchorState(AnchorState.CONTENT_DEDUPLICATION);
        return deduplicationParam;

    }
}
