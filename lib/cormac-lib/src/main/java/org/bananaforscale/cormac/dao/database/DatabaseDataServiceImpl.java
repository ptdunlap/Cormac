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
package org.bananaforscale.cormac.dao.database;

import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.bananaforscale.cormac.dao.AbstractDataService;
import org.bananaforscale.cormac.exception.datasource.DatasourceException;
import org.bananaforscale.cormac.exception.datasource.ExistsException;
import org.bananaforscale.cormac.exception.datasource.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Paul Dunlap
 */
public class DatabaseDataServiceImpl extends AbstractDataService implements DatabaseDataService {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseDataServiceImpl.class);

    public DatabaseDataServiceImpl(final MongoClient mongoClient) {
        super(mongoClient);
    }

    /**
     * Returns a list of all database names present on the server.
     *
     * @return the names of databases present on this server
     * @throws DatasourceException
     */
    @Override
    public List<String> getDatabases() throws DatasourceException {
        try {
            List<String> dbList = new ArrayList<>();
            dbList.addAll(getDatabaseNames());
            return dbList;
        } catch (MongoException ex) {
            logger.error("An error occured while retrieving database list", ex);
            throw new DatasourceException("An error occured while retrieving database list");
        }
    }

    /**
     * Returns statistics that reflect the use state of a single database.
     *
     * @param databaseName the database
     * @return A JSON string with statistics reflecting the database state.
     * @throws DatasourceException
     * @throws NotFoundException
     */
    @Override
    public String getDatabaseStats(String databaseName) throws DatasourceException, NotFoundException {
        try {
            if (!databaseExists(databaseName)) {
                throw new NotFoundException("The database doesn't exist in the datasource");
            }
            // TODO: Find equivalent in java api for MongoDatabase
            DB mongoDatabase = mongoClient.getDB(databaseName);
            DBObject statsObject = mongoDatabase.getStats();
            return JSON.serialize(statsObject);
        } catch (MongoException ex) {
            logger.error("An error occured while retrieving database stats", ex);
            throw new DatasourceException("An error occured while retrieving database stats");
        }
    }

    /**
     * Creates a new database explicitly. Because MongoDB creates a database implicitly when the
     * database is first referenced in a command, this method is not required for usage of said
     * database.
     *
     * @param databaseName the database to create
     * @return the result of the operation
     * @throws DatasourceException
     * @throws ExistsException
     */
    @Override
    public boolean addDatabase(String databaseName) throws DatasourceException, ExistsException {
        try {
            if (databaseExists(databaseName)) {
                throw new ExistsException("The database already exists in the datasource");
            }
            MongoDatabase mongoDatabase = mongoClient.getDatabase(databaseName);
            String collectionName = "temp" + UUID.randomUUID();
            mongoDatabase.createCollection(collectionName);
            mongoDatabase.getCollection(collectionName).drop();
            return true;
        } catch (MongoException ex) {
            logger.error("An error occured while adding the database", ex);
            throw new DatasourceException("An error occured while adding the database");
        }
    }

    /**
     * Removes a database with a given name.
     *
     * @param databaseName the database
     * @return the result of the operation
     * @throws DatasourceException
     * @throws NotFoundException
     */
    @Override
    public boolean removeDatabase(String databaseName) throws DatasourceException, NotFoundException {
        try {
            if (!databaseExists(databaseName)) {
                throw new NotFoundException("The database doesn't exist in the datasource");
            }
            mongoClient.getDatabase(databaseName).drop();
            return true;
        } catch (MongoException ex) {
            logger.error("An error occured while removing the database", ex);
            throw new DatasourceException("An error occured while removing the database");
        }
    }
}
