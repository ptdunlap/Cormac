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
package org.bananaforscale.cormac.dao.document;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import org.bananaforscale.cormac.dao.AbstractDataService;
import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.bananaforscale.cormac.exception.datasource.DatasourceException;
import org.bananaforscale.cormac.exception.datasource.NotFoundException;
import org.bananaforscale.cormac.exception.serialization.DeserializeException;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Paul Dunlap
 */
public class DocumentDataServiceImpl extends AbstractDataService implements DocumentDataService {

    private static final Logger logger = LoggerFactory.getLogger(DocumentDataServiceImpl.class);

    public DocumentDataServiceImpl(MongoClient mongoClient) {
        super(mongoClient);
    }

    /**
     * Returns all the documents in a collection.
     *
     * @param databaseName the database
     * @param collectionName the collection
     * @param query a JSON query param in the style of mongo
     * @param fields fields to return
     * @param skip the amount of documents to skip
     * @param limit the amount of documents to limit the result to
     * @param orderBy order ascending or descending by property
     * @param includeId determines whether to include the Mongo "_id" field
     * @return the documents in a collection
     * @throws DatasourceException
     * @throws NotFoundException
     */
    @Override
    public List<String> getAll(String databaseName, String collectionName, String query, String fields, String skip, String limit, String orderBy, boolean includeId)
            throws DatasourceException, NotFoundException {
        try {
            if (!databaseExists(databaseName)) {
                throw new NotFoundException("The database doesn't exist in the datasource");
            }

            if (!collectionExists(databaseName, collectionName)) {
                throw new NotFoundException("The collection doesn't exist in the datasource");
            }
            Integer intSkip, intLimit;
            try {
                intSkip = Integer.parseInt(skip);
            } catch (NumberFormatException ex) {
                intSkip = 0;
            }
            try {
                intLimit = Integer.parseInt(limit);
            } catch (NumberFormatException ex) {
                intLimit = 0;
            }

            // 1 or -1 to specify an ascending or descending sort respectively.
            Document orderByObject = null;
            if (orderBy != null && !orderBy.isEmpty()) {
                if (orderBy.contains("ascending")) {
                    String[] parts = orderBy.split(":");
                    orderByObject = new Document(parts[0], 1);
                } else if (orderBy.contains("descending")) {
                    String[] parts = orderBy.split(":");
                    orderByObject = new Document(parts[0], -1);
                }
            }
            MongoDatabase mongoDatabase = mongoClient.getDatabase(databaseName);
            MongoCollection collection = mongoDatabase.getCollection(collectionName);
            FindIterable iterable = (query == null || query.isEmpty())
                    ? collection.find() : collection.find(Document.parse(query));

            // TODO: Figure out how to do this in new API
//            if (fields != null && !fields.isEmpty()) {
//                // expect the form to be field:value,field:value
//                Document document = new Document();
//                String[] parts = fields.split(",");
//                for (String part : parts) {
//                    String[] tempParts = part.split(":");
//                    document.append(tempParts[0], tempParts[1]);
//                }
//                iterable.projection(document);
//            }
            iterable.skip(intSkip);
            iterable.limit(intLimit);
            if (orderByObject != null) {
                iterable.sort(orderByObject);
            }
            Iterator<Document> curIter = iterable.iterator();
            List<String> documentList = new ArrayList<>();
            while (curIter.hasNext()) {
                Document current = curIter.next();
                if (!includeId) {
                    current.remove("_id");
                }
                documentList.add(JSON.serialize(current));
            }
            return documentList;
        } catch (MongoException ex) {
            logger.error("An error occured while retrieving the document list", ex);
            throw new DatasourceException("An error occured while retrieving the document list");
        }
    }

    /**
     * Returns the document of the given document identifier.
     *
     * @param databaseName the database
     * @param collectionName the collection
     * @param documentId the document identifier to query for
     * @return the document of the given identifier
     * @throws DatasourceException
     * @throws NotFoundException
     */
    @Override
    public String getById(String databaseName, String collectionName, String documentId)
            throws DatasourceException, NotFoundException {
        try {
            if (!databaseExists(databaseName)) {
                throw new NotFoundException("The database doesn't exist in the datasource");
            }
            if (!collectionExists(databaseName, collectionName)) {
                throw new NotFoundException("The collection doesn't exist in the datasource");
            }
            MongoDatabase mongoDatabase = mongoClient.getDatabase(databaseName);
            MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
            Document query = new Document("_id", new ObjectId(documentId));
            if (collection.count(query) == 0) {
                throw new NotFoundException("The document doesn't exist in the datasource");
            }
            Document document = collection.find(query).first();
            document.remove("_id");
            return JSON.serialize(document);
        } catch (MongoException ex) {
            logger.error("An error occured while retrieving the document", ex);
            throw new DatasourceException("An error occured while retrieving the document");
        }
    }

    /**
     * Saves a document to the collection. If the specified database and
     * collection do not exist they will be created.
     *
     * @param databaseName the database
     * @param collectionName the collection
     * @param content the JSON payload
     * @return the document identifier
     * @throws DatasourceException
     * @throws DeserializeException
     * @throws IllegalArgumentException
     */
    @Override
    public String add(String databaseName, String collectionName, String content)
            throws DatasourceException, DeserializeException, IllegalArgumentException {
        try {
            if (!validInputForAddOrUpdate(databaseName, collectionName, "temp", content)) {
                throw new IllegalArgumentException();
            }
            MongoDatabase mongoDatabase = mongoClient.getDatabase(databaseName);
            MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
            Document document = Document.parse(content);
            collection.insertOne(document);
            return document.get("_id").toString();
        } catch (IllegalArgumentException | ClassCastException | JSONParseException ex) {
            logger.error("The JSON payload is invalid", ex);
            throw new DeserializeException("The JSON payload is invalid");
        } catch (MongoException ex) {
            logger.error("An error occured while adding the document", ex);
            throw new DatasourceException("An error occured while adding the document");
        }
    }

    /**
     * Updates a document in the collection. If the document exists in the
     * collection it will be updated. If the document doesn't exist an error
     * will be thrown. If the specified database and collection do not exist
     * they will be created.
     *
     * @param databaseName the database
     * @param collectionName the collection
     * @param documentId the document identifier
     * @param content the JSON payload
     * @return a status message with the outcome of the operation
     * @throws DatasourceException
     * @throws DeserializeException
     * @throws IllegalArgumentException
     * @throws NotFoundException
     */
    @Override
    public boolean replaceById(String databaseName, String collectionName, String documentId, String content)
            throws DatasourceException, DeserializeException, IllegalArgumentException, NotFoundException {
        try {
            if (!validInputForAddOrUpdate(databaseName, collectionName, documentId, content)) {
                throw new IllegalArgumentException();
            }
            MongoDatabase mongoDatabase = mongoClient.getDatabase(databaseName);
            MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
            Document query = new Document("_id", new ObjectId(documentId));
            Document document = Document.parse(content);
            if (collection.count(query) == 0) {
                throw new NotFoundException("The document doesn't exist in the collection");
            }
            collection.replaceOne(query, document);
            return true;
        } catch (IllegalArgumentException | ClassCastException | JSONParseException ex) {
            logger.error("The JSON payload is invalid", ex);
            throw new DeserializeException("The JSON payload is invalid");
        } catch (MongoException ex) {
            logger.error("An error occured while updating the document", ex);
            throw new DatasourceException("An error occured while updating the document");
        }
    }

    /**
     * Removes a document in the collection.
     *
     * @param databaseName the database
     * @param collectionName the collection
     * @param documentId the document identifier to delete
     * @return the result of the operation
     * @throws DatasourceException
     * @throws NotFoundException
     */
    @Override
    public boolean deleteById(String databaseName, String collectionName, String documentId)
            throws DatasourceException, NotFoundException {
        try {
            if (!databaseExists(databaseName)) {
                throw new NotFoundException("The database doesn't exist in the datasource");
            }
            if (!collectionExists(databaseName, collectionName)) {
                throw new NotFoundException("The collection doesn't exist in the datasource");
            }
            MongoDatabase mongoDatabase = mongoClient.getDatabase(databaseName);
            MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
            Document query = new Document("_id", new ObjectId(documentId));
            if (collection.count(query) == 0) {
                throw new NotFoundException("The document doesn't exist in the datasource");
            }
            collection.deleteOne(query);
            return true;
        } catch (MongoException ex) {
            logger.error("An error occured while updating the document", ex);
            throw new DatasourceException("An error occured while updating the document");
        }
    }

    /**
     * Removes all documents in the collection. Not the most efficient approach
     * but if you have a collection that was created with certain options and
     * want to clear everything out this will preserve the configuration. As the
     * new Java API requires you to iterate and delete its more efficient to use
     * the old API.
     *
     * @param databaseName the database
     * @param collectionName the collection
     * @return the result of the operation
     * @throws DatasourceException
     * @throws NotFoundException
     */
    @Override
    public boolean deleteAll(String databaseName, String collectionName)
            throws DatasourceException, NotFoundException {
        try {

            if (!databaseExists(databaseName)) {
                throw new NotFoundException("The database doesn't exist in the datasource");
            }
            if (!collectionExists(databaseName, collectionName)) {
                throw new NotFoundException("The collection doesn't exist in the datasource");
            }
            DB mongoDatabase = mongoClient.getDB(databaseName);
            DBCollection collection = mongoDatabase.getCollection(collectionName);
            collection.remove(new BasicDBObject());
            return true;
        } catch (MongoException ex) {
            logger.error("An error occured while updating the document", ex);
            throw new DatasourceException("An error occured while updating the document");
        }
    }

    /**
     * The form upload for documents could lead to invalid parameters being
     * passed in so this method performs a sanity check on the values
     *
     * @return
     */
    private boolean validInputForAddOrUpdate(String databaseName,
            String collectionName, String documentId, String content) {
        if (databaseName == null || databaseName.isEmpty()) {
            return false;
        } else if (collectionName == null || collectionName.isEmpty()) {
            return false;
        } else if (documentId == null || documentId.isEmpty()) {
            return false;
        } else if (content == null || content.isEmpty()) {
            return false;
        }
        return true;
    }
}
