package com.java3y.austin.support.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.setting.dialect.Props;
import com.ctrip.framework.apollo.Config;
import com.java3y.austin.support.service.ConfigService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;


/**
 * @author 3y
 * 读取配置实现类
 */
@Service
public class ConfigServiceImpl implements ConfigService {

    /**
     * 本地配置
     */
   // private static final String PROPERTIES_PATH = "local.properties";
    private static final String PROPERTIES_PATH = "application.properties";

    private Props props = new Props(PROPERTIES_PATH, StandardCharsets.UTF_8);

    /**
     * apollo配置
     */
    @Value("${apollo.bootstrap.enabled}")
    private Boolean enableApollo;
    @Value("${apollo.bootstrap.namespaces}")
    private String namespaces;

    @Override
    public String getProperty(String key, String defaultValue) {

        if (enableApollo && key == "smsAccount") {

            return props.getProperty(key, defaultValue);

        }else if(enableApollo && key == "enterpriseWechatAccount") {
            return props.getProperty(key, defaultValue);
        }else if(enableApollo && key == "emailAccount") {
            return props.getProperty(key, defaultValue);

        }else if(enableApollo && key == "dingDingWorkNoticeAccount") {
            return props.getProperty(key, defaultValue);

        } else if(enableApollo && key == "dingDingRobotAccount") {

            return props.getProperty(key, defaultValue);
        } else if(enableApollo && key != "smsAccount" && key != "emailAccount"&& key != "dingDingWorkNoticeAccount" && key != "dingDingRobotAccount" && key != "enterpriseWechatAccount"){
            Config config = com.ctrip.framework.apollo.ConfigService.getConfig(namespaces.split(StrUtil.COMMA)[0]);

            return config.getProperty(key, defaultValue);
        } else {


            return props.getProperty(key, defaultValue);

        }
    }

    /**
     * 读取本地配置
     * @param key
     * @param defaultValue
     * @return
     */

    @Value("${smsAccount}")
    private String  sms;

    @Override
    public String getPropertyLocal(String key, String defaultValue) {
        System.out.println("========新方法===========");
        System.out.println(sms);
        return sms;
    }


}
