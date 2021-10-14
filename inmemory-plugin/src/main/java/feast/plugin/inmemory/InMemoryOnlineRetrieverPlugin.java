/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2021 The Feast Authors
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
package feast.plugin.inmemory;

import com.google.common.collect.ImmutableList;
import feast.proto.serving.ServingAPIProto;
import feast.storage.api.retriever.Feature;
import feast.storage.api.retriever.OnlineRetrieverV2;
import java.util.List;
import org.pf4j.Extension;
import org.pf4j.PluginWrapper;
import org.pf4j.spring.SpringPlugin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class InMemoryOnlineRetrieverPlugin extends SpringPlugin {
  public InMemoryOnlineRetrieverPlugin(PluginWrapper wrapper) {
    super(wrapper);
  }

  public OnlineRetrieverV2 retrieverV2;

  @Override
  public void start() {
    System.out.println("InMemoryOnlineRetriever.start()");
    retrieverV2 = getApplicationContext().getBean(OnlineRetrieverV2.class);

    System.out.println("RetrieverV2: " + retrieverV2);
    System.out.println(getApplicationContext().getApplicationName());

    System.out.print("All defined beans: ");
    for (final String bean : getApplicationContext().getBeanDefinitionNames()) {
      System.out.println(bean);
    }
  }

  @Override
  public void stop() {
    System.out.println("InMemoryOnlineRetriever.stop()");
    super.stop(); // to close applicationContext
  }

  @Override
  protected ApplicationContext createApplicationContext() {
    AnnotationConfigApplicationContext applicationContext =
        new AnnotationConfigApplicationContext();
    applicationContext.setClassLoader(getClass().getClassLoader());
    applicationContext.register(PluginConfiguration.class);
    applicationContext.refresh();

    return applicationContext;
  }

  @Extension
  public static class InMemoryOnlineRetriever implements OnlineRetrieverV2 {

    @Autowired ApplicationContext applicationContext;

    @Override
    public List<List<Feature>> getOnlineFeatures(
        String project,
        List<ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow> entityRows,
        List<ServingAPIProto.FeatureReferenceV2> featureReferences,
        List<String> entityNames) {
      System.out.println("In plugin getOnlineFeatures");
      System.out.println("App Config:" + this.applicationContext.getApplicationName());
      return ImmutableList.of();
    }
  }
}
