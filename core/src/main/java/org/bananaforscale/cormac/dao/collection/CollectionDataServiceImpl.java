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
package org.bananaforscale.cormac.dao.collection;

import com.mongodb.MongoClient;
import org.bananaforscale.cormac.dao.AbstractDataService;
import com.mongodb.MongoException;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import java.util.ArrayList;
import java.util.List;
import org.bananaforscale.cormac.exception.datasource.DatasourceException;
import org.bananaforscale.cormac.exception.datasource.ExistsException;
import org.bananaforscale.cormac.exception.datasource.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Paul Dunlap
 */
public class CollectionDataServiceImpl extends AbstractDataService implements CollectionDataService {

    private static final Logger logger = LoggerFactory.getLogger(CollectionDataServiceImpl.class);

    public CollectionDataServiceImpl(MongoClient mongoClient) {
        super(mongoClient);
    }

    /**
     * Returns the names of all collections in this database.
     *
     * @param databaseName the database
     * @return the names of collections in this database
     * @throws DatasourceException
     * @throws NotFoundException
     */
    @Override
    public List<String> getCollections(String databaseName) throws DatasourceException, NotFoundException {
        try {
            if (!databaseExists(databaseName)) {
                throw new NotFoundException("The database doesn't exist in the datasource");
            }
            List<String> collectionList = new ArrayList<>();
            collectionList.addAll(getCollectionNames(getCollectionNames(databaseName)));
            return collectionList;
        } catch (MongoException ex) {
            logger.error("An error occured while retrieving the collection list", ex);
            throw new DatasourceException("An error occured while retrieving the collection list");
        }
    }

    /**
     * Creates a new collection explicitly. Because MongoDB creates a collection
     * implicitly when the collection is first referenced in a command, this
     * method is not required for usage of said collection.
     *
     * @param databaseName the database
     * @param collectionName the collection to create
     * @return the result of the operation
     * @throws DatasourceException
     * @throws ExistsException
     * @throws NotFoundException
     */
    @Override
    public boolean addCollection(String databaseName, String collectionName)
            throws DatasourceException, ExistsException, NotFoundException {
        try {
            if (!databaseExists(databaseName)) {
                throw new NotFoundException("The database doesn't exist in the datasource");
            }
            if (collectionExists(databaseName, collectionName)) {
                throw new ExistsException("The collection already exists in the datasource");
            }
            MongoDatabase mongoDatabase = mongoClient.getDatabase(databaseName);
            CreateCollectionOptions options = new CreateCollectionOptions();
            options.capped(false);
            mongoDatabase.createCollection(collectionName, options);
            return true;
        } catch (MongoException ex) {
            logger.error("An error occured while adding the collection", ex);
            throw new DatasourceException("An error occured while adding the collection");
        }
    }

    /**
     * Removes a collection with a given name.
     *
     * @param databaseName the database
     * @param collectionName the collection to remove
     * @return the result of the operation
     * @throws DatasourceException
     * @throws NotFoundException
     */
    @Override
    public boolean removeCollection(String databaseName, String collectionName)
            throws DatasourceException, NotFoundException {
        try {
            if (!databaseExists(databaseName)) {
                throw new NotFoundException("The database doesn't exist in the datasource");
            }
            if (!collectionExists(databaseName, collectionName)) {
                throw new NotFoundException("The collection doesn't exist in the datasource");
            }
            MongoDatabase mongoDatabase = mongoClient.getDatabase(databaseName);
            mongoDatabase.getCollection(collectionName).drop();
            return true;
        } catch (MongoException ex) {
            logger.error("An error occured while deleting the collection", ex);
            throw new DatasourceException("An error occured while deleting the collection");
        }
    }
}
