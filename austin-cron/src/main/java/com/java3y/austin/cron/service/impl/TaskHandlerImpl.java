package com.java3y.austin.cron.service.impl;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.text.csv.CsvRow;
import cn.hutool.core.util.StrUtil;
import com.java3y.austin.cron.csv.CountFileRowHandler;
import com.java3y.austin.cron.pending.CrowdBatchTaskPending;
import com.java3y.austin.cron.service.TaskHandler;
import com.java3y.austin.cron.utils.ReadFileUtils;
import com.java3y.austin.cron.vo.CrowdInfoVo;
import com.java3y.austin.support.dao.MessageTemplateDao;
import com.java3y.austin.support.domain.MessageTemplate;
import com.java3y.austin.support.pending.AbstractLazyPending;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.util.HashMap;

/**
 * @author 3y
 * @date 2022/2/9
 */
@Service
@Slf4j
public class TaskHandlerImpl implements TaskHandler {
    @Autowired
    private MessageTemplateDao messageTemplateDao;

    @Autowired
    private ApplicationContext context;


    @Override
    public void handle(Long messageTemplateId) {

        MessageTemplate messageTemplate = messageTemplateDao.findById(messageTemplateId).get();

        if (messageTemplate == null || StrUtil.isBlank(messageTemplate.getCronCrowdPath())) {
            log.error("TaskHandler#handle crowdPath empty! messageTemplateId:{}", messageTemplateId);
            return;
        }

        // 1. 获取文件行数大小
        long countCsvRow = ReadFileUtils.countCsvRow(messageTemplate.getCronCrowdPath(), new CountFileRowHandler());

        // 2. 读取文件得到每一行记录给到队列做lazy batch处理 ，每行文件有三个属性  userId,content,url
        CrowdBatchTaskPending crowdBatchTaskPending = context.getBean(CrowdBatchTaskPending.class);

        ReadFileUtils.getCsvRow(messageTemplate.getCronCrowdPath(), row -> {
            if (CollUtil.isEmpty(row.getFieldMap())
                    || StrUtil.isBlank(row.getFieldMap().get(ReadFileUtils.RECEIVER_KEY))) {
                return;
            }

            // 3. 每一行处理交给LazyPending，返回参数map
            HashMap<String, String> params = ReadFileUtils.getParamFromLine(row.getFieldMap());
            //每一行csv的记录构建成一个CrowdInfoVo对象
            CrowdInfoVo crowdInfoVo = CrowdInfoVo.builder().receiver(row.getFieldMap().get(ReadFileUtils.RECEIVER_KEY))
                    .params(params).messageTemplateId(messageTemplateId).build();

            /**
             * 每个CrowdInfoVo对象有三个参数{
             *              1.消息模板Id  --------   messageTemplateId
             *              2.接收者id     -------  receiver
             *              3.参数信息    --------     Map<String, String> params
             *
             *                  }
             */

            /**
             * 把文件每一行数据封装成crowdInfoVo对象后。放进阻塞队列，每当从文件读取一行，我就处理一行
             *
             * 拿到每一行数据的时候，封装了一个VO，又扔给了内存队列LazyPending
             */

            crowdBatchTaskPending.pending(crowdInfoVo);

            // 4. 判断是否读取文件完成回收资源且更改状态
            onComplete(row, countCsvRow, crowdBatchTaskPending, messageTemplateId);
        });



    }

    /**
     * 文件遍历结束时
     * 1. 暂停单线程池消费(最后会回收线程池资源)
     * 2. 更改消息模板的状态(暂未实现)
     *
     * @param row
     * @param countCsvRow
     * @param crowdBatchTaskPending
     * @param messageTemplateId
     */
    private void onComplete(CsvRow row, long countCsvRow, AbstractLazyPending crowdBatchTaskPending, Long messageTemplateId) {
        if (row.getOriginalLineNumber() == countCsvRow) {
            crowdBatchTaskPending.setStop(true);
            log.info("messageTemplate:[{}] read csv file complete!", messageTemplateId);
        }
    }
}
