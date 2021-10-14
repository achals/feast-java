/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package feast.serving;

import com.google.common.collect.ImmutableList;
import feast.serving.config.FeastProperties;
import feast.storage.api.retriever.OnlineRetrieverV2;
import java.lang.reflect.Field;
import org.pf4j.Plugin;
import org.pf4j.PluginManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;

@SpringBootApplication(
    exclude = {
      DataSourceAutoConfiguration.class,
      DataSourceTransactionManagerAutoConfiguration.class,
      HibernateJpaAutoConfiguration.class
    })
@EnableConfigurationProperties(FeastProperties.class)
public class ServingApplication {
  public static final Logger log = LoggerFactory.getLogger(ServingApplication.class);

  public static void main(String[] args) {
    ApplicationContext applicationContext = SpringApplication.run(ServingApplication.class, args);
    PluginManager pluginManager = applicationContext.getBean(PluginManager.class);
    System.out.println("OnlineRetrieverV2 size: " + pluginManager.getPlugins().size());

    final Plugin plugin = pluginManager.getPlugins().get(0).getPlugin();
    try {
      System.out.println("Attempting to access field");
      for (final Field f : plugin.getClass().getDeclaredFields()) {
        System.out.println("Field: " + f.getName());
      }
      Field field = plugin.getClass().getField("retrieverV2");
      OnlineRetrieverV2 r = (OnlineRetrieverV2) field.get(plugin);
      System.out.println(
          r.getOnlineFeatures("", ImmutableList.of(), ImmutableList.of(), ImmutableList.of()));
    } catch (final NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
