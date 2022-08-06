package com.java3y.austin.service.api.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;


/**
 * 发送接口返回值
 * @author 3y
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
public class SendResponse {



    /**
     * 响应状态
     */
    private String code;

    /**
     * 响应编码
     */
    private String msg;

}
