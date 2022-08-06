package com.java3y.austin.support.pending;

import cn.hutool.core.collection.CollUtil;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.java3y.austin.support.config.SupportThreadPoolConfig;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 延迟消费 阻塞队列-消费者和生产者实现
 *
 * @author 3y
 */
@Slf4j
@Data
public abstract class AbstractLazyPending<T> {

    /**
     * 子类构造方法必须初始化该参数
     */
    protected PendingParam<T> pendingParam;

    /**
     * 批量装载任务
     */
    private List<T> tasks = new ArrayList<>();

    /**
     * 上次执行的时间
     */
    private Long lastHandleTime = System.currentTimeMillis();

    /**
     * 是否终止线程
     */
    private Boolean stop = false;

    /**
     * 单线程消费 阻塞队列的数据
     */
    @PostConstruct
    public void initConsumePending() {
        ExecutorService executorService = SupportThreadPoolConfig.getPendingSingleThreadPool();
        executorService.execute(() -> {
            while (true) {

                try {
                    /**
                     * poll(long var1, TimeUnit var3)：从BlockingQueue首位取出数据，如果在指定的时间内，队列一旦有数据可取，就立即返回队列中的数据，否则超时返回null
                     */
                    T obj = pendingParam.getQueue().poll(pendingParam.getTimeThreshold(), TimeUnit.MILLISECONDS);
                    if (null != obj) {
                        tasks.add(obj);
                    }

                    // 处理条件：1. 数量超限 2. 时间超限,满足这两个条件的任意一个才会调用handle方法
                    /**
                     *         tasks.size() >= pendingParam.getNumThreshold() ||
                     *
                     *  (System.currentTimeMillis() - lastHandleTime >= pendingParam.getTimeThreshold());
                     *
                     *
                     *
                     *  内存队列里会起一个线程消费队列里的数据，等到积压到给定的size或者timeout就会给到实际消费者进行处理
                     *
                     *  我这样设计的目的在于：我要调用批量发送接口，使用内存队列作为介质实现生产者和消费者模式为了做batch处理。具体来说：如果我每读取一行就调用一次发送接口，假设人群有2000W，我就需要调用2000W次。
                     *
                     *
                     */
                    if (CollUtil.isNotEmpty(tasks) && dataReady()) {
                        List<T> taskRef = tasks;

                        tasks = Lists.newArrayList();

                        lastHandleTime = System.currentTimeMillis();

                        // 具体执行逻辑
                        pendingParam.getExecutorService().execute(() -> this.handle(taskRef));
                    }

                    // 判断是否停止当前线程
                    if (stop && CollUtil.isEmpty(tasks)) {
                        executorService.shutdown();
                        break;
                    }
                } catch (Exception e) {
                    log.error("Pending#initConsumePending failed:{}", Throwables.getStackTraceAsString(e));
                }

            }
        });
    }

    /**
     * 1. 数量超限
     * 2. 时间超限
     *
     * @return
     */
    private boolean dataReady() {
        return tasks.size() >= pendingParam.getNumThreshold() ||
                (System.currentTimeMillis() - lastHandleTime >= pendingParam.getTimeThreshold());
    }

    /**
     * 将元素放入阻塞队列中
     *
     * @param t
     */
    public void pending(T t) {
        try {
            pendingParam.getQueue().put(t);
        } catch (InterruptedException e) {
            log.error("Pending#pending error:{}", Throwables.getStackTraceAsString(e));
        }
    }

    /**
     * 消费阻塞队列元素时的方法
     *
     * @param t
     */
    public void handle(List<T> t) {
        if (t.isEmpty()) {
            return;
        }
        try {
            doHandle(t);
        } catch (Exception e) {
            log.error("Pending#handle failed:{}", Throwables.getStackTraceAsString(e));
        }
    }

    /**
     * 处理阻塞队列的元素 真正方法
     *
     * @param list
     */
    public abstract void doHandle(List<T> list);

}
