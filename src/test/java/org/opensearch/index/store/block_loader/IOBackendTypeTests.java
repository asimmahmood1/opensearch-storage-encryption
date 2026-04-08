/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_loader;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.amazonaws.juno.settings.IOBackendType;


public class IOBackendTypeTests {

    @Test
    public void FromStringValidValues() {
        assertEquals(IOBackendType.FILE_CHANNEL, IOBackendType.fromString("FILE_CHANNEL"));
        assertEquals(IOBackendType.POSIX_PREAD, IOBackendType.fromString("POSIX_PREAD"));
        assertEquals(IOBackendType.IO_URING, IOBackendType.fromString("IO_URING"));
    }

    @Test
    public void FromStringInvalidValueThrows() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
            () -> IOBackendType.fromString("INVALID"));
        assertTrue(e.getMessage().contains("Invalid io_backend value"));
        assertTrue(e.getMessage().contains("INVALID"));
    }

    @Test
    public void FromStringLowercaseThrows() {
        assertThrows(IllegalArgumentException.class,
            () -> IOBackendType.fromString("file_channel"));
    }

    @Test
    public void FromStringEmptyThrows() {
        assertThrows(IllegalArgumentException.class,
            () -> IOBackendType.fromString(""));
    }

    @Test
    public void FromStringNullThrows() {
        assertThrows(NullPointerException.class,
            () -> IOBackendType.fromString(null));
    }
}
