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
package org.bananaforscale.cormac;

/**
 * Encapsulates the configuration for the Cormac Web Application
 *
 * @author ptdunlap
 */
public class Configuration {

    private String mongoServer;
    private boolean useCORS;
    private boolean useUniqueIds;

    public String getMongoServer() {
        return mongoServer;
    }

    public void setMongoServer(String mongoServer) {
        this.mongoServer = mongoServer;
    }

    public boolean isUseCORS() {
        return useCORS;
    }

    public void setUseCORS(boolean useCORS) {
        this.useCORS = useCORS;
    }

    public boolean isUseUniqueIds() {
        return useUniqueIds;
    }

    public void setUseUniqueIds(boolean useUniqueIds) {
        this.useUniqueIds = useUniqueIds;
    }

}
