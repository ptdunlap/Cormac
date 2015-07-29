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
package org.bananaforscale.cormac.dao.gridfs;

import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.MongoDatabase;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;
import com.mongodb.util.JSON;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.tika.Tika;
import org.bananaforscale.cormac.dao.AbstractDataService;
import org.bananaforscale.cormac.exception.datasource.DatasourceException;
import org.bananaforscale.cormac.exception.datasource.ExistsException;
import org.bananaforscale.cormac.exception.datasource.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to store and retrieve files using {@link GridFS}. This class still makes use of the old
 * {@link DB} as it seems the new Java API hasn't switched over to using {@link MongoDatabase}.
 */
public class GridFsDataServiceImpl extends AbstractDataService implements GridFsDataService {

    private static final Logger logger = LoggerFactory.getLogger(GridFsDataServiceImpl.class);

    private final Tika tika = new Tika();

    public GridFsDataServiceImpl(MongoClient mongoClient) {
        super(mongoClient);
    }

    /**
     * Returns the names of all buckets in this database.
     *
     * @param databaseName the database
     * @return the names of buckets in this database
     * @throws DatasourceException
     * @throws NotFoundException
     */
    @Override
    public List<String> getBuckets(String databaseName) throws DatasourceException, NotFoundException {
        try {
            if (!databaseExists(databaseName)) {
                throw new NotFoundException("The database doesn't exist in the datasource");
            }
            List<String> bucketList = new ArrayList<>();
            bucketList.addAll(getBucketNames(getCollectionNames(databaseName)));
            return bucketList;
        } catch (MongoException ex) {
            logger.error("An error occured while retrieving bucket list", ex);
            throw new DatasourceException("An error occured while retrieving bucket list");
        }
    }

    /**
     * Adds a bucket to the database.
     *
     * @param databaseName the database
     * @param bucketName the bucket
     * @return the result of the operation
     * @throws DatasourceException
     * @throws ExistsException
     * @throws NotFoundException
     */
    @Override
    public boolean addBucket(String databaseName, String bucketName)
            throws DatasourceException, ExistsException, NotFoundException {
        try {
            if (!databaseExists(databaseName)) {
                throw new NotFoundException("The database doesn't exist in the datasource");
            }
            if (bucketExists(databaseName, bucketName)) {
                throw new ExistsException("The bucket already exists in the database");
            }
            DB mongoDatabase = mongoClient.getDB(databaseName);
            new GridFS(mongoDatabase, bucketName);
            return true;
        } catch (MongoException ex) {
            logger.error("An error occured while adding a bucket", ex);
            throw new DatasourceException("An error occured while adding a bucket");
        }
    }

    /**
     * Deletes a bucket from the database.
     *
     * @param databaseName the database
     * @param bucketName the bucket
     * @return the result of the operation
     * @throws DatasourceException
     * @throws NotFoundException
     */
    @Override
    public boolean removeBucket(String databaseName, String bucketName)
            throws DatasourceException, NotFoundException {
        try {
            if (!databaseExists(databaseName)) {
                throw new NotFoundException("The database doesn't exist in the datasource");
            }
            if (!bucketExists(databaseName, bucketName)) {
                throw new NotFoundException("The bucket doesn't exist in the database");
            }
            MongoDatabase mongoDatabase = mongoClient.getDatabase(databaseName);
            String chunks = bucketName + ".chunks";
            String files = bucketName + ".files";
            mongoDatabase.getCollection(chunks).drop();
            mongoDatabase.getCollection(files).drop();
            return true;
        } catch (MongoException ex) {
            logger.error("An error occured while deleting a bucket", ex);
            throw new DatasourceException("An error occured while deleting a bucket");
        }

    }

    /**
     * Returns all the files in a bucket.
     *
     * @param databaseName the database
     * @param bucketName the bucket
     * @return the files in the bucket
     * @throws DatasourceException
     * @throws NotFoundException
     */
    @Override
    public List<String> getAll(String databaseName, String bucketName)
            throws DatasourceException, NotFoundException {
        try {
            if (!databaseExists(databaseName)) {
                throw new NotFoundException("The database doesn't exist in the datasource");
            }
            if (!bucketExists(databaseName, bucketName)) {
                throw new NotFoundException("The bucket doesn't exist in the database");
            }
            DB mongoDatabase = mongoClient.getDB(databaseName);
            GridFS gfsBucket = new GridFS(mongoDatabase, bucketName);
            DBCursor cursor = gfsBucket.getFileList();
            Iterator<DBObject> curIter = cursor.iterator();
            List<String> fileList = new ArrayList<>();
            while (curIter.hasNext()) {
                DBObject current = curIter.next();
                fileList.add(JSON.serialize(current));
            }
            return fileList;
        } catch (MongoException ex) {
            logger.error("An error occured while retrieving file list", ex);
            throw new DatasourceException("An error occured while retrieving file list");
        }

    }

    /**
     * Removes all files in a bucket.
     *
     * @param databaseName the database
     * @param bucketName the bucket
     * @return a status message with the outcome of the operation
     * @throws DatasourceException
     * @throws NotFoundException
     */
    @Override
    public boolean removeAll(String databaseName, String bucketName)
            throws DatasourceException, NotFoundException {
        try {
            if (!databaseExists(databaseName)) {
                throw new NotFoundException("The database doesn't exist in the datasource");
            }
            DB mongoDatabase = mongoClient.getDB(databaseName);
            GridFS gfsBucket = new GridFS(mongoDatabase, bucketName); // TODO: determine behavior if bucket doesnt exist
            DBCursor cursor = gfsBucket.getFileList();
            Iterator<DBObject> curIter = cursor.iterator();
            while (curIter.hasNext()) {
                DBObject current = curIter.next();
                gfsBucket.remove(current);
            }
            return true;
        } catch (MongoException ex) {
            logger.error("An error occured while removing files", ex);
            throw new DatasourceException("An error occured while removing files");
        }
    }

    /**
     * Saves a file to the database by file name. This is used during a form upload. We use tika to
     * determine the content type.
     *
     * TODO: Refactor this mess
     *
     * @param databaseName the name of the database
     * @param bucketName the name of the bucket
     * @param fileName the name of the file
     * @param overwrite whether to overwrite an existing file with the same name
     * @param stream the file byte stream
     * @return the Mongo ID of the file
     * @throws DatasourceException
     * @throws ExistsException
     * @throws NotFoundException
     */
    @Override
    public String addByForm(String databaseName, String bucketName, String fileName, boolean overwrite, InputStream stream)
            throws DatasourceException, ExistsException, NotFoundException {
        String fileId = null;
        try {
            if (!databaseExists(databaseName)) {
                throw new NotFoundException("The database doesn't exist in the datasource");
            }
            DB mongoDatabase = mongoClient.getDB(databaseName);
            GridFS gfsBucket = new GridFS(mongoDatabase, bucketName);
            GridFSDBFile gfsFile = gfsBucket.findOne(fileName);
            if (gfsFile == null) {
                // the file does not exist -- create
                GridFSInputFile dbFile = gfsBucket.createFile(stream, fileName);
                dbFile.setContentType(tika.detect(fileName));
                dbFile.save();
                fileId = dbFile.getId().toString();
            } else {
                // the file exists
                if (overwrite) {
                    // overwrite the existing file
                    gfsBucket.remove(gfsFile);
                    GridFSInputFile inputFile = gfsBucket.createFile(stream, fileName);
                    inputFile.setContentType(tika.detect(fileName));
                    inputFile.save();
                    fileId = inputFile.getId().toString();
                } else {
                    throw new ExistsException("The file already exists in the bucket");
                }
            }
        } catch (MongoException ex) {
            logger.error("Could not persist entity to bucket", ex);
            throw new DatasourceException("Could not persist file to bucket");
        }
        if (fileId == null || fileId.isEmpty()) {
            throw new DatasourceException("Could not persist file to bucket");
        }
        return fileId;
    }

    /**
     * Saves a document to the database by file name. If the document already exists this request
     * will be dropped and the existing file will not be overwritten.
     *
     * @param databaseName the database
     * @param bucketName the bucket
     * @param fileName the file name
     * @param inputStream the binary payload
     * @return the identifier of the file
     * @throws DatasourceException
     * @throws ExistsException
     * @throws NotFoundException
     */
    @Override
    public String addByFileName(String databaseName, String bucketName, String fileName, InputStream inputStream)
            throws DatasourceException, ExistsException, NotFoundException {
        try {
            if (!databaseExists(databaseName)) {
                throw new NotFoundException("The database doesn't exist in the datasource");
            }
            DB mongoDatabase = mongoClient.getDB(databaseName);
            GridFS gfsBucket = new GridFS(mongoDatabase, bucketName);
            if (gfsBucket.findOne(fileName) != null) {
                throw new ExistsException("The file already exists");
            }
            GridFSInputFile inputFile = gfsBucket.createFile(inputStream, fileName);
            inputFile.setContentType(tika.detect(fileName));
            inputFile.save();
            return inputFile.getId().toString();
        } catch (MongoException ex) {
            logger.error("An error occured while adding the file", ex);
            throw new DatasourceException("An error occured while adding the file");
        }
    }

    /**
     * Updates a file in the database. If the file exists in the database it will be updated. If the
     * file doesn't exist it will be created.
     *
     * @param databaseName the database
     * @param bucketName the bucket
     * @param fileName the file name
     * @param inputStream the binary payload
     * @return the identifier of the file
     * @throws DatasourceException
     * @throws NotFoundException
     */
    @Override
    public String updateByFileName(String databaseName, String bucketName, String fileName, InputStream inputStream)
            throws DatasourceException, NotFoundException {
        try {
            if (!databaseExists(databaseName)) {
                throw new NotFoundException("The database doesn't exist in the datasource");
            }
            DB mongoDatabase = mongoClient.getDB(databaseName);
            GridFS gfsBucket = new GridFS(mongoDatabase, bucketName);
            GridFSDBFile gfsFile = gfsBucket.findOne(fileName);
            if (gfsFile == null) {
                GridFSInputFile inputFile = gfsBucket.createFile(inputStream, fileName);
                inputFile.setContentType(tika.detect(fileName));
                inputFile.save();
                return inputFile.getId().toString();
            } else {
                gfsBucket.remove(gfsFile);
                GridFSInputFile inputFile = gfsBucket.createFile(inputStream, fileName);
                inputFile.setContentType(tika.detect(fileName));
                inputFile.save();
                return inputFile.getId().toString();
            }
        } catch (MongoException ex) {
            logger.error("An error occured while updating the file", ex);
            throw new DatasourceException("An error occured while updating the file");
        }
    }

    /**
     * Returns the file with the given file name.
     *
     * @param databaseName the database
     * @param bucketName the bucket
     * @param fileName the file name
     * @return the file in with the given file name
     * @throws DatasourceException
     * @throws IOException
     * @throws NotFoundException
     */
    @Override
    public FileEnvelope getByFileName(String databaseName, String bucketName, String fileName)
            throws DatasourceException, IOException, NotFoundException {
        try {
            if (!databaseExists(databaseName)) {
                throw new NotFoundException("The database doesn't exist in the datasource");
            }
            DB mongoDatabase = mongoClient.getDB(databaseName);
            GridFS gfsBucket = new GridFS(mongoDatabase, bucketName);
            GridFSDBFile gfsFile = gfsBucket.findOne(fileName);
            if (gfsFile == null) {
                throw new NotFoundException("The file doesnt exist");
            }
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            gfsFile.writeTo(baos);

            return new FileEnvelope(baos.toByteArray(), gfsFile.getContentType(), fileName);
        } catch (MongoException ex) {
            logger.error("An error occured while retrieving the file", ex);
            throw new DatasourceException("An error occured while retrieving the file");
        }
    }

    /**
     * Removes a file in the database.
     *
     * @param databaseName the database
     * @param bucketName the bucket
     * @param fileName the file to delete
     * @return a status message with the outcome of the operation
     * @throws DatasourceException
     * @throws NotFoundException
     */
    @Override
    public boolean removeByFileName(String databaseName, String bucketName, String fileName)
            throws DatasourceException, NotFoundException {
        try {
            if (!databaseExists(databaseName)) {
                throw new NotFoundException("The database doesn't exist in the datasource");
            }
            DB mongoDatabase = mongoClient.getDB(databaseName);
            GridFS gfsBucket = new GridFS(mongoDatabase, bucketName);
            GridFSDBFile gfsFile = gfsBucket.findOne(fileName);
            if (gfsFile == null) {
                throw new NotFoundException("The file doesnt exist");
            }
            gfsBucket.remove(gfsFile);
            return true;
        } catch (MongoException ex) {
            logger.error("An error occured while removing the file", ex);
            throw new DatasourceException("An error occured while removing the file");
        }
    }
}
