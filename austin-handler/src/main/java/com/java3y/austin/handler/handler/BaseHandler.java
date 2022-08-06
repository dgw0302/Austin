package com.java3y.austin.handler.handler;

import com.google.common.util.concurrent.RateLimiter;
import com.java3y.austin.common.domain.AnchorInfo;
import com.java3y.austin.common.domain.TaskInfo;
import com.java3y.austin.common.enums.AnchorState;
import com.java3y.austin.handler.enums.RateLimitStrategy;
import com.java3y.austin.handler.flowcontrol.FlowControlParam;
import com.java3y.austin.handler.flowcontrol.FlowControlService;
import com.java3y.austin.support.utils.LogUtils;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;

/**
 * @author 3y
 * 发送各个渠道的handler
 */
public abstract class BaseHandler implements Handler {
    @Autowired
    private HandlerHolder handlerHolder;
    @Autowired
    private LogUtils logUtils;
    @Autowired
    private FlowControlService flowControlService;

    /**
     * 标识渠道的Code
     * 子类初始化的时候指定
     */
    protected Integer channelCode;

    /**
     * 限流相关的参数
     * 子类初始化的时候指定
     */
    protected FlowControlParam flowControlParam;

    /**
     *     初始化渠道与Handler的映射关系
     * 　　 子类实例化过程中会调用父类中的@PostConstruct方法。（这样就把各个渠道具体的handler注入到HandlerHolder里面的map里面（key:ChannelType value:对应具体的handler））
     */
    @PostConstruct
    private void init() {
        handlerHolder.putHandler(channelCode, this);
    }

    /**
     * 流量控制
     *
     * @param taskInfo
     */
    public void flowControl(TaskInfo taskInfo) {
        // 只有子类指定了限流参数，才需要限流
        if (flowControlParam != null) {
            flowControlService.flowControl(taskInfo, flowControlParam);
        }
    }
    @Override
    public void doHandler(TaskInfo taskInfo) {

        /**
         * 限流
         *    (10, "根据真实请求数限流"),
         *          (20, "根据发送用户数限流"),
         */
        flowControl(taskInfo);
        if (handler(taskInfo)) {
            logUtils.print(AnchorInfo.builder().state(AnchorState.SEND_SUCCESS.getCode()).businessId(taskInfo.getBusinessId()).ids(taskInfo.getReceiver()).build());
            return;
        }
        logUtils.print(AnchorInfo.builder().state(AnchorState.SEND_FAIL.getCode()).businessId(taskInfo.getBusinessId()).ids(taskInfo.getReceiver()).build());
    }




    /**
     * 统一处理的handler接口
     *
     * @param taskInfo
     * @return
     */
    public abstract boolean handler(TaskInfo taskInfo);





}
