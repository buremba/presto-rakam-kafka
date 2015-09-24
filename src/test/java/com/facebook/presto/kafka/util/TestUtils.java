/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.kafka.util;

import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.tests.TestingPrestoClient;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map;
import java.util.Properties;

import static com.facebook.presto.kafka.util.EmbeddedKafka.CloseableProducer;
import static java.lang.String.format;

public final class TestUtils
{
    private TestUtils() {}

    public static int findUnusedPort()
            throws IOException
    {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    public static Properties toProperties(Map<String, String> map)
    {
        Properties properties = new Properties();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            properties.setProperty(entry.getKey(), entry.getValue());
        }
        return properties;
    }

    public static void loadTpchTopic(EmbeddedKafka embeddedKafka, TestingPrestoClient prestoClient, String topicName, QualifiedTableName tpchTableName)
    {
        try (CloseableProducer<Long, Object> producer = embeddedKafka.createProducer();
                KafkaLoader tpchLoader = new KafkaLoader(producer, topicName, prestoClient.getServer(), prestoClient.getDefaultSession())) {
            tpchLoader.execute(format("SELECT * from %s", tpchTableName));
        }
    }
}
