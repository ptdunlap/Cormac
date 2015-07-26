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

import java.io.IOException;
import org.bananaforscale.cormac.dao.gridfs.GridFsDataService;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import org.bananaforscale.cormac.dao.gridfs.FileEnvelope;
import org.bananaforscale.cormac.exception.datasource.DatasourceException;
import org.bananaforscale.cormac.exception.datasource.ExistsException;
import org.bananaforscale.cormac.exception.datasource.NotFoundException;
import org.jboss.resteasy.plugins.providers.multipart.InputPart;
import org.jboss.resteasy.plugins.providers.multipart.MultipartFormDataInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resource to handle Mongo Document operations
 *
 * @author ptdunlap
 */
@Path("gridfs")
public class GridFsResource {

    private static final Logger logger = LoggerFactory.getLogger(GridFsResource.class);
    @Context
    HttpServletRequest request;
    GridFsDataService gds;

    public GridFsResource(GridFsDataService gds) {
        this.gds = gds;
    }

    /**
     * Returns the names of all buckets in this database.
     *
     * @param databaseName the database
     * @return the names of buckets in this database
     */
    @GET
    @Path("{databaseName}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getBuckets(@PathParam("databaseName") String databaseName) {
        try {
            List<String> bucketList = gds.getBuckets(databaseName);
            return Response.ok(bucketList).build();
        } catch (DatasourceException ex) {
            return Response.status(500).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        } catch (NotFoundException ex) {
            return Response.status(400).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        }
    }

    /**
     * Adds a bucket to the database.
     *
     * @param databaseName the database
     * @param bucketName the bucket
     * @return a status message indicating the result of the operation
     */
    @POST
    @Path("{databaseName}/{bucketName}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response addBucket(@PathParam("databaseName") String databaseName,
            @PathParam("bucketName") String bucketName) {
        try {
            boolean result = gds.addBucket(databaseName, bucketName);
            return Response.ok(ResourceUtil.buildJson("ok", String.valueOf(result))).build();
        } catch (DatasourceException ex) {
            return Response.status(500).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        } catch (NotFoundException | ExistsException ex) {
            return Response.status(400).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        }
    }

    /**
     * Removes a bucket to the database.
     *
     * @param databaseName the database
     * @param bucketName the bucket
     * @return a status message indicating the result of the operation
     */
    @DELETE
    @Path("{databaseName}/{bucketName}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeBucket(@PathParam("databaseName") String databaseName,
            @PathParam("bucketName") String bucketName) {
        try {
            boolean result = gds.removeBucket(databaseName, bucketName);
            return Response.ok(ResourceUtil.buildJson("ok", String.valueOf(result))).build();
        } catch (DatasourceException ex) {
            return Response.status(500).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        } catch (NotFoundException ex) {
            return Response.status(400).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        }
    }

    /**
     * Returns all the files in a bucket.
     *
     * @param databaseName the database
     * @param bucketName the bucket
     * @return the files in the bucket
     */
    @GET
    @Path("{databaseName}/{bucketName}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAll(@PathParam("databaseName") String databaseName,
            @PathParam("bucketName") String bucketName) {
        try {
            List<String> fileList = gds.getAll(databaseName, bucketName);
            return Response.ok(ResourceUtil.createJsonArray(fileList)).build();
        } catch (DatasourceException ex) {
            return Response.status(500).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        } catch (NotFoundException ex) {
            return Response.status(400).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        }
    }

    /**
     * Removes all files in a bucket.
     *
     * @param databaseName the database
     * @param bucketName the bucket
     * @return a status message with the outcome of the operation
     */
    @DELETE
    @Path("{databaseName}/{bucketName}/files")
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeAll(@PathParam("databaseName") String databaseName,
            @PathParam("bucketName") String bucketName) {
        try {
            boolean result = gds.removeAll(databaseName, bucketName);
            return Response.ok(ResourceUtil.buildJson("ok", String.valueOf(result))).build();
        } catch (DatasourceException ex) {
            return Response.status(500).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        } catch (NotFoundException ex) {
            return Response.status(400).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        }
    }

    /**
     * Saves a file to the bucket by file name. This is used during a form
     * upload of a file and requires the following form parameters and their
     * corresponding input types: <br />
     *
     * Param: inputFile, type: file, Desc: the file to upload <br/>
     * Param: databaseName, type: text, Desc: the database name<br/>
     * Param: bucketName, type: text, Desc: the bucket name<br/>
     * Param: overwrite, type: checkbox, Desc: Specifies whether to overwrite
     * existing document<br/>
     *
     * @param input
     * @return a status message with the outcome of the operation
     */
    @POST
    @Path("form-upload")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    public Response addByForm(MultipartFormDataInput input) {
        Map<String, List<InputPart>> uploadForm = input.getFormDataMap();
        List<InputPart> inputParts = uploadForm.get("inputFile");
        String result = null;
        for (InputPart inputPart : inputParts) {
            try {
                String databaseName = uploadForm.get("databaseName").get(0).getBodyAsString();
                String bucketName = uploadForm.get("bucketName").get(0).getBodyAsString();
                List<InputPart> checkbox = uploadForm.get("overwrite");
                boolean overwrite = false;
                if (checkbox != null) {
                    overwrite = true;
                }
                MultivaluedMap<String, String> header = inputPart.getHeaders();
                String fileName = ResourceUtil.getFileName(header);
                if (databaseName == null || databaseName.isEmpty()) {
                    logger.error("Could not save file without a database specified.");
                    continue;
                } else if (bucketName == null || bucketName.isEmpty()) {
                    logger.error("Could not save file without a bucket specified.");
                    continue;
                } else if (fileName == null || fileName.isEmpty()) {
                    logger.error("Could not save file without a filename.");
                    continue;
                }
                InputStream inputStream = inputPart.getBody(InputStream.class, null);
                result = gds.addByForm(databaseName, bucketName, fileName, overwrite, inputStream);
            } catch (IOException | DatasourceException ex) {
                return Response.status(500).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
            } catch (ExistsException ex) {
                return Response.status(400).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
            } catch (NotFoundException ex) {
                return Response.status(404).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
            }
        }
        return Response.ok(ResourceUtil.buildJson("id", result)).build();
    }

    /**
     * Saves a document to the bucket by file name. If the document already
     * exists this request will be dropped and the existing file will not be
     * overwritten.
     *
     * @param databaseName the database
     * @param bucketName the bucket
     * @param fileName the file name
     * @param inputStream the binary payload
     * @return a JSON document with the ID of the file
     */
    @POST
    @Path("{databaseName}/{bucketName}/{fileName}")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    public Response addByFileName(@PathParam("databaseName") String databaseName,
            @PathParam("bucketName") String bucketName,
            @PathParam("fileName") String fileName,
            InputStream inputStream) {
        try {
            String result = gds.addByFileName(databaseName, bucketName, fileName, inputStream);
            return Response.ok(ResourceUtil.buildJson("id", result)).build();
        } catch (DatasourceException ex) {
            return Response.status(500).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        } catch (ExistsException ex) {
            return Response.status(400).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        } catch (NotFoundException ex) {
            return Response.status(404).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        }
    }

    /**
     * Updates a file in the bucket. If the file exists in the bucket it will be
     * updated. If the file doesn't exist it will be created.
     *
     * @param databaseName the database
     * @param bucketName the bucket
     * @param fileName the file name
     * @param inputStream the binary payload
     * @return a status message with the outcome of the operation
     */
    @PUT
    @Path("{databaseName}/{bucketName}/{fileName}")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateByFileName(@PathParam("databaseName") String databaseName,
            @PathParam("bucketName") String bucketName,
            @PathParam("fileName") String fileName,
            InputStream inputStream) {
        try {
            String result = gds.updateByFileName(databaseName, bucketName, fileName, inputStream);
            return Response.ok(ResourceUtil.buildJson("id", result)).build();
        } catch (DatasourceException ex) {
            return Response.status(500).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        } catch (NotFoundException ex) {
            return Response.status(404).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        }
    }

    /**
     * Returns the file with the given file name.
     *
     * @param databaseName the database
     * @param bucketName the bucket
     * @param fileName the file name
     * @return the file in with the given file name
     */
    @GET
    @Path("{databaseName}/{bucketName}/{fileName}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getByFileName(@PathParam("databaseName") String databaseName,
            @PathParam("bucketName") String bucketName,
            @PathParam("fileName") String fileName) {
        try {
            FileEnvelope envelope = gds.getByFileName(databaseName, bucketName, fileName);
            Response.ResponseBuilder builder = Response.ok(envelope.getBytes(), envelope.getContentType());
            // Content Disposition attachment prompts the save dialog box.
            // builder.header("Content-Disposition", "attachment;filename=" + fileName);
            // Content Disposition inline will try to open the file in the browser.
            builder.header("Content-Disposition", "inline;filename=" + envelope.getName());
            return builder.build();
        } catch (DatasourceException ex) {
            return Response.status(500).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        } catch (IOException ex) {
            return Response.status(500).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        } catch (NotFoundException ex) {
            return Response.status(404).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        }
    }

    /**
     * Removes a file in the bucket.
     *
     * @param databaseName the database
     * @param bucketName the bucket
     * @param fileName the file to delete
     * @return a status message with the outcome of the operation
     */
    @DELETE
    @Path("{databaseName}/{bucketName}/{fileName}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeByFileName(@PathParam("databaseName") String databaseName,
            @PathParam("bucketName") String bucketName,
            @PathParam("fileName") String fileName) {
        try {
            boolean result = gds.removeByFileName(databaseName, bucketName, fileName);
            return Response.ok(ResourceUtil.buildJson("ok", String.valueOf(result))).build();
        } catch (DatasourceException ex) {
            return Response.status(500).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        } catch (NotFoundException ex) {
            return Response.status(400).entity(ResourceUtil.buildJson("error", ex.getMessage())).build();
        }
    }
}
