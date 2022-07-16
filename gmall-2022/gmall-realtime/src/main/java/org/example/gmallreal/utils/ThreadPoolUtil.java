package org.example.gmallreal.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

//创建 单例 线程池对象的工具类
public class ThreadPoolUtil {
    public static ThreadPoolExecutor pool;

    /**
     * corePoolSize:指定了线程池中的线程数量，它的数量决定了添加的任务是开辟新的线程
     * 去执行，还是放到 workQueue 任务队列中去；
     *  maximumPoolSize:指定了线程池中的最大线程数量，这个参数会根据你使用的
     * workQueue 任务队列的类型，决定线程池会开辟的最大线程数量；
     *  keepAliveTime:当线程池中空闲线程数量超过 corePoolSize 时，多余的线程会在多长时间
     * 内被销毁；
     *  unit:keepAliveTime 的单位
     *  workQueue:任务队列，被添加到线程池中，但尚未被执行的任务
     * @return
     */
    public static ThreadPoolExecutor getInstance(){
        //懒汉式单例模式，会有线程安全的问题，当执行到判空语句时切换线程，可能导致线程不安全。所以需要加锁，双重校验
        if(pool == null){ //不等于空才进方法
            synchronized (ThreadPoolUtil.class){ //对象锁需要加入对象
                if(pool == null){ //进方法之后判断是否已经改变
                    pool = new ThreadPoolExecutor(
                            4,20,300, TimeUnit.SECONDS,new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE)
                    );
                }
            }
        }
        return pool;
    }
}
