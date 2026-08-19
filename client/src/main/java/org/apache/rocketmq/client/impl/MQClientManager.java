/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class MQClientManager {
    private final static Logger log = LoggerFactory.getLogger(MQClientManager.class);

    // 单例模式
    private static MQClientManager instance = new MQClientManager();

    private AtomicInteger factoryIndexGenerator = new AtomicInteger();

    // 在 start 后，创建了 MQClientInstance 放入了这里，对应的 key 是 clientId
    private ConcurrentMap<String/* clientId */, MQClientInstance> factoryTable =
        new ConcurrentHashMap<>();

    private MQClientManager() {

    }

    public static MQClientManager getInstance() {
        return instance;
    }

    public MQClientInstance getOrCreateMQClientInstance(final ClientConfig clientConfig) {
        return getOrCreateMQClientInstance(clientConfig, null);
    }

    /**
     * 创建 mq 客户端实例
     * @param clientConfig
     * @param rpcHook
     * @return
     */
    public MQClientInstance getOrCreateMQClientInstance(final ClientConfig clientConfig, RPCHook rpcHook) {
        // 构建clientId
        String clientId = clientConfig.buildMQClientId();
        // 缓存，第一次默认没有
        // 最后创建了一个 MQClientInstance
        MQClientInstance instance = this.factoryTable.get(clientId);
        if (null == instance) {
            // 创建 config 配置
            // 根据配置新建MQClient
            instance =
                new MQClientInstance(clientConfig.cloneClientConfig(),
                    // 原子计数    clientId     null
                    this.factoryIndexGenerator.getAndIncrement(), clientId, rpcHook);

            // 将 clientId 和创建的 MQClientInstance 放入缓存
            MQClientInstance prev = this.factoryTable.putIfAbsent(clientId, instance);

            // 如果旧的不为空
            if (prev != null) {
                // 则以旧的为准
                instance = prev;
                log.warn("Returned Previous MQClientInstance for clientId:[{}]", clientId);
            } else {
                log.info("Created new MQClientInstance for clientId:[{}]", clientId);
            }
        }

        // 返回实例对象
        return instance;
    }

    public void removeClientFactory(final String clientId) {
        this.factoryTable.remove(clientId);
    }
}
