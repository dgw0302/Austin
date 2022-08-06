package com.java3y.austin.handler.handler.impl;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.spring.annotation.ApolloConfig;
import com.google.common.base.Throwables;
import com.java3y.austin.common.constant.AustinConstant;
import com.java3y.austin.common.domain.TaskInfo;
import com.java3y.austin.common.dto.model.SmsContentModel;
import com.java3y.austin.common.enums.ChannelType;
import com.java3y.austin.handler.domain.sms.MessageTypeSmsConfig;
import com.java3y.austin.handler.domain.sms.SmsParam;
import com.java3y.austin.handler.handler.BaseHandler;
import com.java3y.austin.handler.handler.Handler;
import com.java3y.austin.handler.script.SmsScriptHolder;
import com.java3y.austin.support.dao.SmsRecordDao;
import com.java3y.austin.support.domain.MessageTemplate;
import com.java3y.austin.support.domain.SmsRecord;
import com.java3y.austin.support.service.ConfigService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Random;

/**
 * 短信发送处理
 *
 * @author 3y
 */
@Component
@Slf4j
public class SmsHandler extends BaseHandler implements Handler {

    public SmsHandler() {
        channelCode = ChannelType.SMS.getCode();
    }

    @Autowired
    private SmsRecordDao smsRecordDao;

    @Autowired
    private SmsScriptHolder smsScriptHolder;

    @Autowired
    private ConfigService config;


    @Override
    public boolean handler(TaskInfo taskInfo) {
        /**
         * 构建发送短信参数：
         *           {
         *           1.业务Id
         *           2. 需要发送的手机号    Set<String> phones
         *           3.  发送文案
         *
         *           }
         */
        SmsParam smsParam = SmsParam.builder()
                .phones(taskInfo.getReceiver())
                .content(getSmsContent(taskInfo))
                .messageTemplateId(taskInfo.getMessageTemplateId())
                .build();

        try {
            /**
             * 1、动态配置做流量负载
             * 2、发送短信
             *
             *
             *
             *
             *
             *     * messageTypeSmsConfigs{
             *      *
             *      *                      对于每种消息类型的 短信配置
             *      *                   1.  weights     ---   权重(决定着流量的占比)
             *      *                   2.  scriptName           script名称
             *      *
             *      *
             *      *                      }
             *      *
             *      *
             */

            MessageTypeSmsConfig[] messageTypeSmsConfigs = loadBalance(getMessageTypeSmsConfig(taskInfo.getMsgType()));

            for (MessageTypeSmsConfig messageTypeSmsConfig : messageTypeSmsConfigs) {

                List<SmsRecord> recordList = smsScriptHolder.route(messageTypeSmsConfig.getScriptName()).send(smsParam);

                if (CollUtil.isNotEmpty(recordList)) {
                    smsRecordDao.saveAll(recordList);
                    return true;
                }
            }

        } catch (Exception e) {
            log.error("SmsHandler#handler fail:{},params:{}",
                    Throwables.getStackTraceAsString(e), JSON.toJSONString(smsParam));
        }
        return false;
    }

    /**
     * 流量负载
     * 根据配置的权重优先走某个账号，并取出一个备份的
     *
     *
     *
     *
     *
     * @param messageTypeSmsConfigs
     */
    private MessageTypeSmsConfig[] loadBalance(List<MessageTypeSmsConfig> messageTypeSmsConfigs) {

        int total = 0;
        for (MessageTypeSmsConfig channelConfig : messageTypeSmsConfigs) {
            total += channelConfig.getWeights();
        }

        // 生成一个随机数[1,total]，看落到哪个区间
        Random random = new Random();
        int index = random.nextInt(total) + 1;

        MessageTypeSmsConfig supplier = null;
        MessageTypeSmsConfig supplierBack = null;
        /**
         * messageTypeSmsConfigs.size()：某消息渠道供应商的个数
         */
        for (int i = 0; i < messageTypeSmsConfigs.size(); ++i) {
            if (index <= messageTypeSmsConfigs.get(i).getWeights()) {
                supplier = messageTypeSmsConfigs.get(i);

                // 取下一个供应商
                int j = (i + 1) % messageTypeSmsConfigs.size();
                if (i == j) {
                    return new MessageTypeSmsConfig[]{supplier};
                }
                supplierBack = messageTypeSmsConfigs.get(j);
                return new MessageTypeSmsConfig[]{supplier, supplierBack};
            }
            index -= messageTypeSmsConfigs.get(i).getWeights();
        }
        return null;
    }

    /**
     * 每种类型都会有其下发渠道账号的配置(流量占比也会配置里面)
     * <p>
     * 样例：
     * key：msgTypeSmsConfig
     * value：[{"message_type_10":[{"weights":80,"scriptName":"TencentSmsScript"},{"weights":20,"scriptName":"YunPianSmsScript"}]},{"message_type_20":[{"weights":20,"scriptName":"YunPianSmsScript"}]},{"message_type_30":[{"weights":20,"scriptName":"TencentSmsScript"}]},{"message_type_40":[{"weights":20,"scriptName":"TencentSmsScript"}]}]
     * 通知类短信有两个发送渠道 TencentSmsScript 占80%流量，YunPianSmsScript占20%流量
     * 营销类短信只有一个发送渠道 YunPianSmsScript
     * 验证码短信只有一个发送渠道 TencentSmsScript
     *
     *
     * @param msgType
     * @return
     */
    //对于每种消息类型的 短信配置
    private List<MessageTypeSmsConfig> getMessageTypeSmsConfig(Integer msgType) {

        String apolloKey = "msgTypeSmsConfig";
        String messagePrefix = "message_type_";
        //读取apollo配置
        String property = config.getProperty(apolloKey, AustinConstant.APOLLO_DEFAULT_VALUE_JSON_ARRAY);

        /**
         * json字符串格式是这样的（下面的json数组里面的单个元素格式）
         *     {
         *         "message_type_10": [
         *             {
         *                 "weights": 99,
         *                 "scriptName": "TencentSmsScript"
         *             },
         *             {
         *                 "weights": 1,
         *                 "scriptName": "YunPianSmsScript"
         *             }
         *         ]
         *     }
         *
         *
         */
        JSONArray jsonArray = JSON.parseArray(property);

        for (int i = 0; i < jsonArray.size(); i++) {


            JSONArray array = jsonArray.getJSONObject(i).getJSONArray(messagePrefix + msgType);
            if (CollUtil.isNotEmpty(array)) {
                /**
                 *
                 * 3种消息类型：通知类，营销类，验证码
                 *
                 * MessageTypeSmsConfig就是每种消息类型的短信配置
                 *
                 * 对于每种消息类型的 短信配置
                 * 1.  weights     ---   权重(决定着流量的占比)
                 * 2.  scriptName           script名称{TencentSmsScript     YunPianSmsScript }
                 *
                 *
                 * 将
                 */

                /**
                 *             //将json数组里面的数据拼接字符串："message_type_10“，根据这个message_type_10获取值
                 *
                 *              像上面的例子得到的还是有个数组，两个元素99 和 1
                 *
                 *              将每个元素字符串按照MessageTypeSmsConfig封装成类装进list里面
                 */
                List<MessageTypeSmsConfig> result = JSON.parseArray(JSON.toJSONString(array), MessageTypeSmsConfig.class);
                return result;
            }
        }
        return null;
    }

    /**
     * 如果有输入链接，则把链接拼在文案后
     * <p>
     * PS: 这里可以考虑将链接 转 短链
     * PS: 如果是营销类的短信，需考虑拼接 回TD退订 之类的文案
     */
    private String getSmsContent(TaskInfo taskInfo) {
        SmsContentModel smsContentModel = (SmsContentModel) taskInfo.getContentModel();
        if (StrUtil.isNotBlank(smsContentModel.getUrl())) {
            return smsContentModel.getContent() + " " + smsContentModel.getUrl();
        } else {
            return smsContentModel.getContent();
        }
    }

    @Override
    public void recall(MessageTemplate messageTemplate) {

    }
}
