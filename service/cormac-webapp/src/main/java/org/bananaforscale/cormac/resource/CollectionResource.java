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
import org.bananaforscale.cormac.dao.collection.CollectionDataService;
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
 * Resource to handle Mongo Collection operations
 *
 * @author Paul Dunlap
 */
@Path("collection")
public class CollectionResource {

    private static final Logger logger = LoggerFactory.getLogger(CollectionResource.class);
    private final CollectionDataService cds;
    @Context
    HttpServletRequest request;

    public CollectionResource(CollectionDataService cds) {
        this.cds = cds;
    }

    /**
     * Returns the names of all collections in this database.
     *
     * @param databaseName the database
     * @return the names of collections in this database
     */
    @GET
    @Path("{databaseName}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getCollections(@PathParam("databaseName") String databaseName) {
        try {
            List<String> collectionList = cds.getCollections(databaseName);
            return Response.ok(collectionList).build();
        } catch (DatasourceException ex) {
            return Response.status(500).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        } catch (NotFoundException ex) {
            return Response.status(400).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        }

    }

    /**
     * Creates a new collection explicitly. Because MongoDB creates a collection
     * implicitly when the collection is first referenced in a command, this
     * method is not required for usage of said collection.
     *
     * @param databaseName the database
     * @param collectionName the collection to create
     * @return a status message with the outcome of the operation
     */
    @PUT
    @Path("{databaseName}/{collectionName}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response addCollection(@PathParam("databaseName") String databaseName,
            @PathParam("collectionName") String collectionName) {
        try {
            boolean result = cds.addCollection(databaseName, collectionName);
            return Response.ok(ResourceUtil.buildJson("ok", String.valueOf(result))).build();
        } catch (DatasourceException ex) {
            return Response.status(500).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        } catch (ExistsException ex) {
            return Response.status(400).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        } catch (NotFoundException ex) {
            return Response.status(404).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        }
    }

    /**
     * Deletes a collection with a given name.
     *
     * @param databaseName the database
     * @param collectionName the collection to delete
     * @return a status message with the outcome of the operation
     */
    @DELETE
    @Path("{databaseName}/{collectionName}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeCollection(@PathParam("databaseName") String databaseName,
            @PathParam("collectionName") String collectionName) {
        try {
            boolean result = cds.removeCollection(databaseName, collectionName);
            return Response.ok(ResourceUtil.buildJson("ok", String.valueOf(result))).build();
        } catch (DatasourceException ex) {
            return Response.status(500).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        } catch (NotFoundException ex) {
            return Response.status(404).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        }
    }

}
