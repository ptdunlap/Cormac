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
package org.bananaforscale.cormac.exception.serialization;

/**
 * An {@link Exception} for serialization errors.
 */
public class SerializeException extends Exception {

    /**
     * Message used to build the {@link SerializeException} when no other message is specified.
     */
    static final String DEFAULT_MESSAGE = "The object could not be serialized.";

    /**
     * Initializes the {@link SerializeException} with a default message.
     */
    public SerializeException() {
        super(DEFAULT_MESSAGE);
    }

    /**
     * Initializes the {@link SerializeException} with the specified message.
     *
     * @param message the message
     */
    public SerializeException(final String message) {
        super(message);
    }

}
