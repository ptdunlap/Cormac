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
package org.bananaforscale.cormac.exception.datasource;

/**
 * An {@link Exception} for when there is an update error in a data source.
 */
public class UpdateException extends Exception {

    /**
     * Message used to build the {@link UpdateException} when no other message is specified.
     */
    static final String DEFAULT_MESSAGE = "The record could not be read from the datasource.";

    /**
     * Initializes the {@link UpdateException} with a default message.
     */
    public UpdateException() {
        super(DEFAULT_MESSAGE);
    }

    /**
     * Initializes the {@link UpdateException} with the specified message.
     *
     * @param message the message
     */
    public UpdateException(final String message) {
        super(message);
    }

}
