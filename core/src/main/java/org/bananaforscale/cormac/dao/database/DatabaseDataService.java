/*
 * Copyright 2013 bananaforscale.org
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

import java.util.List;
import org.bananaforscale.cormac.exception.datasource.DatasourceException;
import org.bananaforscale.cormac.exception.datasource.ExistsException;
import org.bananaforscale.cormac.exception.datasource.NotFoundException;

/**
 *
 * @author Paul Dunlap
 */
public interface DatabaseDataService {

    List<String> getDatabases() throws DatasourceException;

    String getDatabaseStats(String databaseName) throws DatasourceException, NotFoundException;

    boolean addDatabase(String databaseName) throws DatasourceException, ExistsException;

    boolean removeDatabase(String databaseName) throws DatasourceException, NotFoundException;
}
