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

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.bananaforscale.cormac.exception.datasource.DatasourceException;
import org.bananaforscale.cormac.exception.datasource.ExistsException;
import org.bananaforscale.cormac.exception.datasource.NotFoundException;

/**
 * Access point for entry related data operations.
 *
 * @author ptdunlap
 */
public interface GridFsDataService {

    List<String> getBuckets(String databaseName)
            throws DatasourceException, NotFoundException;

    boolean addBucket(String databaseName, String bucketName)
            throws DatasourceException, ExistsException, NotFoundException;

    boolean removeBucket(String databaseName, String bucketName)
            throws DatasourceException, NotFoundException;

    List<String> getAll(String databaseName, String bucketName)
            throws DatasourceException, NotFoundException;

    boolean removeAll(String databaseName, String bucketName)
            throws DatasourceException, NotFoundException;

    String addByForm(String databaseName, String bucketName, String fileName, boolean overwrite, InputStream stream)
            throws DatasourceException, ExistsException, NotFoundException;

    String addByFileName(String databaseName, String bucketName, String fileName, InputStream inputStream)
            throws DatasourceException, ExistsException, NotFoundException;

    String updateByFileName(String databaseName, String bucketName, String fileName, InputStream inputStream)
            throws DatasourceException, NotFoundException;

    FileEnvelope getByFileName(String databaseName, String bucketName, String fileName)
            throws DatasourceException, IOException, NotFoundException;

    boolean removeByFileName(String databaseName, String bucketName, String fileName)
            throws DatasourceException, NotFoundException;
}
