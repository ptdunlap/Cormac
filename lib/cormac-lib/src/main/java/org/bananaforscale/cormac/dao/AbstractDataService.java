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
package org.bananaforscale.cormac.dao;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.tika.Tika;

/**
 * Base class for the any DAO which wishes to use the {@code MongoClient}
 *
 * @author ptdunlap
 */
public abstract class AbstractDataService {

    protected final MongoClient mongoClient;
    protected final Tika tika;

    public AbstractDataService(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
        tika = new Tika();
    }

    /**
     * Returns all unique database names in a Mongo data source.
     *
     * @return a list of database names
     */
    protected Set<String> getDatabaseNames() {
        Set<String> dbSet = new HashSet<>();
        MongoCursor<String> cursor = mongoClient.listDatabaseNames().iterator();
        while (cursor.hasNext()) {
            dbSet.add(cursor.next());
        }
        return dbSet;
    }

    /**
     * Determines if a database exists with the specified name.
     *
     * @param databaseName
     * @return whether the database exists
     */
    protected boolean databaseExists(String databaseName) {
        Set<String> dbSet = getDatabaseNames();
        return dbSet.contains(databaseName);
    }

    /**
     * Retrieves the name of the collections in a database.
     *
     * @param databaseName
     * @return a set of collection names
     */
    protected Set<String> getCollectionNames(String databaseName) {
        MongoDatabase mongoDatabase = mongoClient.getDatabase(databaseName);
        Set<String> collectionSet = new HashSet<>();
        MongoCursor<String> cursor = mongoDatabase.listCollectionNames().iterator();
        while (cursor.hasNext()) {
            collectionSet.add(cursor.next());
        }
        return collectionSet;
    }

    /**
     * Determines if a collection exists with the specified name.
     *
     * @param databaseName
     * @param collectionName
     * @return whether the collection exists
     */
    protected boolean collectionExists(String databaseName, String collectionName) {
        Set<String> collectionSet = getCollectionNames(databaseName);
        return collectionSet.contains(collectionName);
    }

    /**
     * Produces a list of collection names omitting buckets, indexes, and users.
     *
     * @param collectionSet
     * @return a list of collection names
     */
    protected List<String> getCollectionNames(Set<String> collectionSet) {
        List<String> collectionList = new ArrayList<>();
        for (String collection : collectionSet) {
            if (!collection.endsWith(".files")
                    && !collection.endsWith(".chunks")
                    && !collection.endsWith(".indexes")
                    && !collection.equals("users")) {
                collectionList.add(collection);
            }
        }
        return collectionList;
    }

    /**
     * Produces a list of the GridFS buckets.
     *
     * @param collectionSet
     * @return a set of bucket names
     */
    protected Set<String> getBucketNames(Set<String> collectionSet) {
        Set<String> bucketList = new HashSet<>();
        for (String collName : collectionSet) {
            if (collName.endsWith(".chunks")) {
                String potentialBucketName = collName.substring(0, collName.indexOf(".chunks"));
                if (collectionSet.contains(potentialBucketName + ".files")) {
                    bucketList.add(potentialBucketName);
                }
            }
        }
        return bucketList;
    }

    /**
     * Determines if a bucket exists with the specified name.
     *
     * @param databaseName
     * @param bucketName
     * @return whether the bucket exists
     */
    protected boolean bucketExists(String databaseName, String bucketName) {
        Set<String> bucketSet = getBucketNames(getCollectionNames(databaseName));
        return bucketSet.contains(bucketName);
    }
}
