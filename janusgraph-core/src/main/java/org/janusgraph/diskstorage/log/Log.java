// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.log;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.StaticBuffer;

import java.util.concurrent.Future;

/**
 * Represents a LOG that allows content to be added to it in the form of messages and to
 * read messages and their content from the LOG via registered {@link MessageReader}s.
 */
public interface Log {

    /**
     * Attempts to add the given content to the LOG and returns a {@link Future} for this action.
     * <p>
     * If the LOG is configured for immediate sending, then any exception encountered during this process is thrown
     * by this method. Otherwise, encountered exceptions are attached to the returned future.
     */
    Future<Message> add(StaticBuffer content);

    /**
     * Attempts to add the given content to the LOG and returns a {@link Future} for this action.
     * In addition, a key is provided to signal the recipient of the LOG message in partitioned logging systems.
     * <p>
     * If the LOG is configured for immediate sending, then any exception encountered during this process is thrown
     * by this method. Otherwise, encountered exceptions are attached to the returned future.
     */
    Future<Message> add(StaticBuffer content, StaticBuffer key);

    /**
     * @param readMarker Indicates where to start reading from the LOG once message readers are registered
     * @param reader     The readers to register (all at once)
     * @see #registerReaders(ReadMarker, Iterable)
     */
    void registerReader(ReadMarker readMarker, MessageReader... reader);

    /**
     * Registers the given readers with this LOG. These readers will be invoked for each newly read message from the LOG
     * starting at the point identified by the provided {@link ReadMarker}.
     * <p>
     * If no previous readers were registered, invoking this method triggers reader threads to be instantiated.
     * If readers have been previously registered, then the provided {@link ReadMarker} must be compatible with the
     * previous {@link ReadMarker} or an exception will be thrown.
     *
     * @param readMarker Indicates where to start reading from the LOG once message readers are registered
     * @param readers    The readers to register (all at once)
     */
    void registerReaders(ReadMarker readMarker, Iterable<MessageReader> readers);

    /**
     * Removes the given reader from the list of registered readers and returns whether this reader was registered in the
     * first place.
     * Note, that removing the last reader does not stop the reading process. Use {@link #close()} instead.
     *
     * @return true if this MessageReader was registered before and was successfully unregistered, else false
     */
    boolean unregisterReader(MessageReader reader);

    /**
     * Returns the name of this LOG
     */
    String getName();

    /**
     * Closes this LOG and stops the reading process.
     */
    void close() throws BackendException;

}
