/*
 * Copyright 2015 bananaforscale.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bananaforscale.cormac;

import com.mongodb.MongoClient;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ServletContextListener used for initializing Cormac configuration properties
 * and also handling the opening and closing of the Mongo connection.
 *
 * @author Paul Dunlap
 */
public class CormacContextServletListener implements ServletContextListener {

    private static final Logger logger = LoggerFactory.getLogger(CormacContextServletListener.class);
    private MongoClient mongoClient;

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        logger.info("Initializing the Cormac Web Application");
        Configuration conf = loadConfiguration(sce);
        mongoClient = new MongoClient(conf.getMongoServer());
        System.out.println("Starting Mongo with address: " + conf.getMongoServer());
        // TODO: add in a connection retry
        final ServletContext context = sce.getServletContext();
        context.setAttribute("mongo-client", mongoClient);
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        logger.info("Shutting down the Cormac Web Application");
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    private Configuration loadConfiguration(ServletContextEvent sce) {
        Configuration conf = new Configuration();
        String mongoServer = System.getProperty("mongo.server");
        mongoServer = (mongoServer == null || mongoServer.isEmpty())
                ? sce.getServletContext().getInitParameter("mongo.server") : mongoServer;
        conf.setMongoServer(mongoServer);
        return conf;
    }
}
