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
import java.util.HashSet;
import java.util.Set;

/**
 * Base class for any DAO needing to use {@link MongoClient}.
 */
public abstract class AbstractDataService {

    protected final MongoClient mongoClient;

    /**
     * @param mongoClient the {@link MongoClient} to use for communicating with MongoDB
     */
    public AbstractDataService(final MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    /**
     * Returns all unique database names in a MongoDB data source.
     *
     * @return a {@link Set} of database names
     */
    protected Set<String> getDatabaseNames() {
        final Set<String> dbSet = new HashSet<>();
        final MongoCursor<String> cursor = mongoClient.listDatabaseNames().iterator();
        while (cursor.hasNext()) {
            dbSet.add(cursor.next());
        }
        return dbSet;
    }

    /**
     * Determines whether a database exists with the specified name.
     *
     * @param databaseName the name of the database to check for
     * @return {@code true} if the database exists, otherwise {@code false}
     */
    protected boolean databaseExists(final String databaseName) {
        final Set<String> dbSet = getDatabaseNames();
        return dbSet.contains(databaseName);
    }

    /**
     * Retrieves the names of the collections in a database.
     *
     * @param databaseName the name of the database
     * @return a {@link Set} of collection names
     */
    protected Set<String> getCollectionNames(final String databaseName) {
        final MongoDatabase mongoDatabase = mongoClient.getDatabase(databaseName);
        final Set<String> collectionSet = new HashSet<>();
        final MongoCursor<String> cursor = mongoDatabase.listCollectionNames().iterator();
        while (cursor.hasNext()) {
            collectionSet.add(cursor.next());
        }
        return collectionSet;
    }

    /**
     * Determines whether a collection with the specified name exists within a database.
     *
     * @param databaseName the name of the database
     * @param collectionName the name of the collection to check for
     * @return {@code true} if the collection exists in the database, otherwise {@code false}
     */
    protected boolean collectionExists(final String databaseName, final String collectionName) {
        final Set<String> collectionSet = getCollectionNames(databaseName);
        return collectionSet.contains(collectionName);
    }

    /**
     * Builds a {@link Set} of collection names omitting buckets, indices, and users.
     *
     * @param collectionSet the {@link Set} of collection names to filter down
     * @return the filtered {@link Set} of collection names
     */
    protected Set<String> getCollectionNames(final Set<String> collectionSet) {
        final Set<String> collectionNames = new HashSet<>();
        for (final String collection : collectionSet) {
            if (!collection.endsWith(".files") && !collection.endsWith(".chunks")
                    && !collection.endsWith(".indexes") && !collection.equals("users")) {
                collectionNames.add(collection);
            }
        }
        return collectionNames;
    }

    /**
     * Builds a {@link Set} of the {@link GridFS} buckets.
     *
     * @param collectionSet a {@link Set} of collection names
     * @return a {@link Set} of bucket names
     */
    protected Set<String> getBucketNames(final Set<String> collectionSet) {
        final Set<String> bucketList = new HashSet<>();
        for (final String collName : collectionSet) {
            if (collName.endsWith(".chunks")) {
                final String potentialBucketName =
                        collName.substring(0, collName.indexOf(".chunks"));
                if (collectionSet.contains(potentialBucketName + ".files")) {
                    bucketList.add(potentialBucketName);
                }
            }
        }
        return bucketList;
    }

    /**
     * Determines whether a bucket with the specified name exists.
     *
     * @param databaseName name of the database to search
     * @param bucketName name of the bucket to check for
     * @return {@code true} if the bucket exists, otherwise {@code false}
     */
    protected boolean bucketExists(final String databaseName, final String bucketName) {
        final Set<String> bucketSet = getBucketNames(getCollectionNames(databaseName));
        return bucketSet.contains(bucketName);
    }

}
