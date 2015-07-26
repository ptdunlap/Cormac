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
import javax.ws.rs.core.MultivaluedMap;

/**
 *
 * @author ptdunlap
 */
public class ResourceUtil {

    /**
     * Builds a simple JSON document to be used as content for a HTTP Response.
     *
     * @param key
     * @param value
     * @return
     */
    protected static String buildJson(String key, String value) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"").append(key).append("\":\"").append(value).append("\"}");
        return sb.toString();
    }

    /**
     * Takes in a list of JSON Objects represented as strings and creates a
     * string representation of a JSON Array .
     *
     * @param originalList
     * @return
     */
    protected static String createJsonArray(List<String> originalList) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < originalList.size(); i++) {
            sb.append(originalList.get(i));
            if (i < (originalList.size() - 1)) {
                sb.append(", ");
            }
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * header sample { Content-Type=[image/png], Content-Disposition=[form-data;
     * name="file"; filename="filename.extension"] }
     *
     */
    protected static String getFileName(final MultivaluedMap<String, String> header) {
        final String[] contentDisposition
                = header.getFirst("Content-Disposition").split(";");
        for (final String filename : contentDisposition) {
            if (filename.trim().startsWith("filename")) {
                final String[] name = filename.split("=");
                final String finalFileName
                        = name[1].trim().replaceAll("\"", "");
                return finalFileName;
            }
        }
        return "unknown";
    }

}
