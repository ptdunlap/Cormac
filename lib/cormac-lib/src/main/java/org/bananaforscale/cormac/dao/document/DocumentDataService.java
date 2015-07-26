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

import java.util.List;
import org.bananaforscale.cormac.exception.datasource.DatasourceException;
import org.bananaforscale.cormac.exception.datasource.NotFoundException;
import org.bananaforscale.cormac.exception.serialization.DeserializeException;

/**
 *
 * @author Paul Dunlap
 */
public interface DocumentDataService {

    public List<String> getAll(String databaseName, String collectionName, String query, String fields, String skip, String limit, String orderBy, boolean includeId)
            throws DatasourceException, NotFoundException;

    String getById(String databaseName, String collectionName, String documentId)
            throws DatasourceException, NotFoundException;

    String add(String databaseName, String collectionName, String content)
            throws DatasourceException, DeserializeException, IllegalArgumentException;

    boolean replaceById(String databaseName, String collectionName, String documentId, String content)
            throws DatasourceException, DeserializeException, IllegalArgumentException, NotFoundException;

    boolean deleteById(String databaseName, String collectionName, String documentId)
            throws DatasourceException, NotFoundException;

    boolean deleteAll(String databaseName, String collectionName)
            throws DatasourceException, NotFoundException;
}
