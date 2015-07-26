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

import org.bananaforscale.cormac.resource.CollectionResource;
import org.bananaforscale.cormac.resource.DatabaseResource;
import org.bananaforscale.cormac.resource.DocumentResource;
import org.bananaforscale.cormac.resource.GridFsResource;
import com.mongodb.MongoClient;
import java.util.HashSet;
import java.util.Set;
import javax.servlet.ServletContext;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import org.bananaforscale.cormac.dao.collection.CollectionDataServiceImpl;
import org.bananaforscale.cormac.dao.database.DatabaseDataServiceImpl;
import org.bananaforscale.cormac.dao.document.DocumentDataServiceImpl;
import org.bananaforscale.cormac.dao.gridfs.GridFsDataServiceImpl;

/**
 * The REST application main class. This class is used to add new resources to
 * be exposed via the REST service.
 *
 * @author ptdunlap
 */
public class CormacApplication extends Application {

    private final HashSet<Object> singletons = new HashSet<>();

    /**
     * Constructs an instance of {@code RESTProviderApplication}.
     *
     * @param sc the injected servlet context
     */
    public CormacApplication(@Context final ServletContext sc) {
        MongoClient mongoClient = (MongoClient) sc.getAttribute("mongo-client");;
        singletons.add(new DatabaseResource(new DatabaseDataServiceImpl(mongoClient)));
        singletons.add(new CollectionResource(new CollectionDataServiceImpl(mongoClient)));
        singletons.add(new DocumentResource(new DocumentDataServiceImpl(mongoClient)));
        singletons.add(new GridFsResource(new GridFsDataServiceImpl(mongoClient)));
    }

    /**
     * {@inheritDoc}
     *
     * @return
     */
    public Set<Class<?>> getClasses() {
        HashSet<Class<?>> set = new HashSet<Class<?>>();
        return set;
    }

    /**
     * {@inheritDoc}
     *
     * @return
     */
    public Set<Object> getSingletons() {
        return singletons;
    }
}
