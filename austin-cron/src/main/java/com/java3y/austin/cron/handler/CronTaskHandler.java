package com.java3y.austin.cron.handler;

import com.dtp.core.thread.DtpExecutor;
import com.java3y.austin.cron.config.CronAsyncThreadPoolConfig;
import com.java3y.austin.cron.service.TaskHandler;
import com.java3y.austin.support.utils.ThreadPoolUtils;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.ThreadPoolExecutor;


/**
 * 后台提交的定时任务处理类
 * @author 3y
 */
@Service
@Slf4j
public class CronTaskHandler {

    @Autowired
    private TaskHandler taskHandler;

    @Autowired
    private ThreadPoolUtils threadPoolUtils;

    /**
     * 业务：接收到xxl-job请求的线程池
     * 配置：不丢弃消息，核心线程数不会随着keepAliveTime而减少(不会被回收)
     * 动态线程池且被Spring管理：true
     *
     *   返回线程池
     * @return
     */
    private DtpExecutor dtpExecutor = CronAsyncThreadPoolConfig.getXxlCronExecutor();

    /**
     * 处理后台的 austin 定时任务消息
     */
    @XxlJob("austinJob")
    public void execute() {
        log.info("CronTaskHandler#execute messageTemplateId:{} cron exec!", XxlJobHelper.getJobParam());
        threadPoolUtils.register(dtpExecutor);

        Long messageTemplateId = Long.valueOf(XxlJobHelper.getJobParam());
        /**
         *
         * 先把数据按行疯转成一个CrowdInfoVo对象，再将一个一个CrowdInfoVo对象放进队列里面LinkedBlockingQueue。
         *
         * 再根据LinkedBlockingQueue队列构建队列，也就是消费对列里面的数据{
         *              tasks.size() >= pendingParam.getNumThreshold() ||
         *                                                                        (System.currentTimeMillis() - lastHandleTime >= pendingParam.getTimeThreshold());
         *              每次从队列取得数据达到batch 触发执行的数量阈值 或者   （当前时间 - 上一次执行时间）大于batch 触发执行的时间阈值，单位毫秒
         *
         *      组装参数MessageParam
         *
         *      封装BatchSendRequest调用接入层接口发送参数 {
         *
         *      * 执行业务类型
         *      * 必传,参考 BusinessCode枚举 {普通发送和撤回消息}
         *
         *
         *      消息模板Id   Long messageTemplateId;
         *
         *
         *
         *      扩展参数  MList<MessageParam> messageParamList;
         *
         *
         *      }
         *      我这样设计的目的在于：我要调用批量发送接口，使用内存队列作为介质实现生产者和消费者模式为了做batch处理。具体来说：如果我每读取一行就调用一次发送接口，假设人群有2000W，我就需要调用2000W次。          }
         *
         *
         *
         * 处理xxl-job接口由线程池处理
         *
         * 消费阻塞队列有线程池消费
         *
         * 在具体执行消费的时候，我是设计了「线程池」进行接口调用的（batchsend），
         *
         *
         *
         *
         *
         *
         *
         *
         */
        dtpExecutor.execute(() -> taskHandler.handle(messageTemplateId));



    }

}
