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
package org.bananaforscale.cormac.resource;

import java.util.List;
import org.bananaforscale.cormac.dao.database.DatabaseDataService;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.bananaforscale.cormac.exception.datasource.DatasourceException;
import org.bananaforscale.cormac.exception.datasource.ExistsException;
import org.bananaforscale.cormac.exception.datasource.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resource to handle Mongo Database operations
 *
 * @author Paul Dunlap
 */
@Path("database")
public class DatabaseResource {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseResource.class);
    @Context
    HttpServletRequest request;
    DatabaseDataService dds;

    public DatabaseResource(DatabaseDataService dds) {
        this.dds = dds;
    }

    /**
     * Returns a list of all database names present on the server.
     *
     * @return the names of databases present on this server
     */
    @GET
    @Path("")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDatabases() {
        try {
            List<String> databaseList = dds.getDatabases();
            return Response.ok(databaseList).build();
        } catch (DatasourceException ex) {
            return Response.status(500).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        }

    }

    /**
     * Creates a new database explicitly. Because MongoDB creates a database
     * implicitly when the database is first referenced in a command, this
     * method is not required for usage of said database.
     *
     * @param databaseName the database to create
     * @return a status message with the outcome of the operation
     */
    @PUT
    @Path("{databaseName}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response addDatabase(@PathParam("databaseName") String databaseName) {
        try {
            boolean result = dds.addDatabase(databaseName);
            return Response.ok(ResourceUtil.buildJson("ok", String.valueOf(result))).build();
        } catch (DatasourceException ex) {
            return Response.status(500).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        } catch (ExistsException ex) {
            return Response.status(400).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        }
    }

    /**
     * Removes a database with a given name.
     *
     * @param databaseName the database
     * @return a status message with the outcome of the operation
     */
    @DELETE
    @Path("{databaseName}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeDatabase(@PathParam("databaseName") String databaseName) {
        try {
            boolean result = dds.removeDatabase(databaseName);
            return Response.ok(ResourceUtil.buildJson("ok", String.valueOf(result))).build();
        } catch (DatasourceException ex) {
            return Response.status(500).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        } catch (NotFoundException ex) {
            return Response.status(400).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        }
    }

    /**
     * Returns statistics that reflect the use state of a single database.
     *
     * @param databaseName the database
     * @return A document with statistics reflecting the database system’s
     * state.
     */
    @GET
    @Path("{databaseName}/stats")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDatabaseStats(@PathParam("databaseName") String databaseName) {
        try {
            String result = dds.getDatabaseStats(databaseName);
            return Response.ok(result).build();
        } catch (DatasourceException ex) {
            return Response.status(500).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        } catch (NotFoundException ex) {
            return Response.status(400).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        }
    }
}
