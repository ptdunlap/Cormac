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
import org.bananaforscale.cormac.dao.document.DocumentDataService;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.bananaforscale.cormac.exception.datasource.DatasourceException;
import org.bananaforscale.cormac.exception.datasource.NotFoundException;
import org.bananaforscale.cormac.exception.serialization.DeserializeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resource to handle Mongo Document operations
 *
 * @author Paul Dunlap
 */
@Path("document")
public class DocumentResource {

    private static final Logger logger = LoggerFactory.getLogger(DocumentResource.class);
    private final DocumentDataService dds;
    @Context
    HttpServletRequest request;

    public DocumentResource(DocumentDataService dds) {
        this.dds = dds;
    }

    /**
     * Returns all the documents in a collection.
     *
     * @param databaseName the database
     * @param collectionName the collection
     * @param fields fields to return (not working yet)
     * @param skip the amount of cases to skip
     * @param limit the amount of cases to limit the result to
     * @param orderBy order ascending or descending by property
     * @param includeId determines whether to include the Mongo "_id" field
     * @return the documents in a collection
     */
    @GET
    @Path("{databaseName}/{collectionName}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAll(@PathParam("databaseName") String databaseName,
            @PathParam("collectionName") String collectionName,
            @QueryParam("fields") String fields,
            @QueryParam("skip") String skip,
            @QueryParam("limit") String limit,
            @QueryParam("orderBy") String orderBy,
            @QueryParam("includeId") String includeId) {
        try {
            boolean include = Boolean.valueOf(includeId);
            List<String> documentList = dds.getAll(databaseName, collectionName, null, fields, skip, limit, orderBy, include);
            return Response.ok(ResourceUtil.createJsonArray(documentList)).build();
        } catch (DatasourceException ex) {
            return Response.status(500).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        } catch (NotFoundException ex) {
            return Response.status(404).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        }
    }

    /**
     * Returns all the documents in a collection that match the query contained
     * within the body.
     *
     * @param databaseName the database
     * @param collectionName the collection
     * @param query a JSON query param in the style of mongo
     * @param fields fields to return
     * @param skip the amount of cases to skip
     * @param limit the amount of cases to limit the result to
     * @param orderBy order ascending or descending by property
     * @param includeId determines whether to include the Mongo "_id" field
     * @return the documents in a collection
     */
    @POST
    @Path("{databaseName}/{collectionName}/query")
    @Produces(MediaType.APPLICATION_JSON)
    public Response queryAll(@PathParam("databaseName") String databaseName,
            @PathParam("collectionName") String collectionName,
            @QueryParam("fields") String fields,
            @QueryParam("skip") String skip,
            @QueryParam("limit") String limit,
            @QueryParam("orderBy") String orderBy,
            @QueryParam("includeId") String includeId,
            String query) {
        try {
            boolean include = Boolean.valueOf(includeId);
            List<String> documentList = dds.getAll(databaseName, collectionName, query, fields, skip, limit, orderBy, include);
            return Response.ok(ResourceUtil.createJsonArray(documentList)).build();
        } catch (DatasourceException ex) {
            return Response.status(500).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        } catch (NotFoundException ex) {
            return Response.status(404).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        }
    }

    /**
     * Returns the document of the given document identifier.
     *
     * @param databaseName the database
     * @param collectionName the collection
     * @param documentId the document identifier to query for
     * @return the document of the given identifier
     */
    @GET
    @Path("{databaseName}/{collectionName}/{documentId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getById(@PathParam("databaseName") String databaseName,
            @PathParam("collectionName") String collectionName,
            @PathParam("documentId") String documentId) {
        try {
            String document = dds.getById(databaseName, collectionName, documentId);
            return Response.ok(document).build();
        } catch (DatasourceException ex) {
            return Response.status(500).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        } catch (NotFoundException ex) {
            return Response.status(404).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        }
    }

    /**
     * Saves a document to the collection. If the specified database and
     * collection do not exist they will be created.
     *
     * @param databaseName the database
     * @param collectionName the collection
     * @param documentId the document identifier
     * @param content the JSON payload
     * @param overwrite specifies whether to overwrite the document if it
     * exists.
     * @return the id of the document
     */
    @POST
    @Path("document-upload")
    @Produces(MediaType.APPLICATION_JSON)
    public Response addByForm(@FormParam("databaseName") String databaseName,
            @FormParam("collectionName") String collectionName,
            @FormParam("documentId") String documentId,
            @FormParam("content") String content,
            @FormParam("overwrite") boolean overwrite) {
        try {
            if (overwrite) {
                boolean result = dds.replaceById(databaseName, collectionName, documentId, content);
                return Response.ok(ResourceUtil.buildJson("ok", String.valueOf(result))).build();
            } else {
                String result = dds.add(databaseName, collectionName, content);
                return Response.ok(ResourceUtil.buildJson("id", result)).build();
            }
        } catch (DatasourceException | DeserializeException ex) {
            return Response.status(500).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        } catch (IllegalArgumentException | NotFoundException ex) {
            return Response.status(400).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        }
    }

    /**
     * Saves a document to the collection. If the specified database and
     * collection do not exist they will be created.
     *
     * @param databaseName the database
     * @param collectionName the collection
     * @param content the JSON payload
     * @return the id of the document
     */
    @POST
    @Path("{databaseName}/{collectionName}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response add(@PathParam("databaseName") String databaseName,
            @PathParam("collectionName") String collectionName, String content) {
        try {
            String result = dds.add(databaseName, collectionName, content);
            return Response.ok(ResourceUtil.buildJson("id", result)).build();
        } catch (DatasourceException | DeserializeException ex) {
            return Response.status(500).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        } catch (IllegalArgumentException ex) {
            return Response.status(400).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        }
    }

    /**
     * Replaces a document in the collection. If the document exists in the
     * collection it will be replaced. If the document doesn't exist an error
     * will be thrown.
     *
     * @param databaseName the database
     * @param collectionName the collection
     * @param documentId the document identifier
     * @param content the JSON payload
     * @return a status message with the outcome of the operation
     */
    @PUT
    @Path("{databaseName}/{collectionName}/{documentId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response replaceById(@PathParam("databaseName") String databaseName,
            @PathParam("collectionName") String collectionName,
            @PathParam("documentId") String documentId, String content) {
        try {
            boolean result = dds.replaceById(databaseName, collectionName, documentId, content);
            return Response.ok(ResourceUtil.buildJson("ok", String.valueOf(result))).build();
        } catch (DatasourceException | DeserializeException ex) {
            return Response.status(500).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        } catch (IllegalArgumentException | NotFoundException ex) {
            return Response.status(400).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        }

    }

    /**
     * Removes a document in the collection.
     *
     * @param databaseName the database
     * @param collectionName the collection
     * @param documentId the document identifier to delete
     * @return a status message with the outcome of the operation
     */
    @DELETE
    @Path("{databaseName}/{collectionName}/{documentId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeById(@PathParam("databaseName") String databaseName,
            @PathParam("collectionName") String collectionName,
            @PathParam("documentId") String documentId) {
        try {
            boolean result = dds.deleteById(databaseName, collectionName, documentId);
            return Response.ok(ResourceUtil.buildJson("ok", String.valueOf(result))).build();
        } catch (DatasourceException ex) {
            return Response.status(500).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        } catch (NotFoundException ex) {
            return Response.status(404).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        }
    }

    /**
     * Removes all the documents in the collection.
     *
     * @param databaseName the database
     * @param collectionName the collection
     * @return a status message with the outcome of the operation
     */
    @DELETE
    @Path("{databaseName}/{collectionName}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeAll(@PathParam("databaseName") String databaseName,
            @PathParam("collectionName") String collectionName) {
        try {
            boolean result = dds.deleteAll(databaseName, collectionName);
            return Response.ok(ResourceUtil.buildJson("ok", String.valueOf(result))).build();
        } catch (DatasourceException ex) {
            return Response.status(500).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        } catch (NotFoundException ex) {
            return Response.status(404).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        }
    }
}
