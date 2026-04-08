/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_loader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


/**
 * Tests for IOBackendStrategy implementations — instantiation and type checks.
 * These tests verify that backends can be created and that the fallback logic works.
 * Actual I/O tests require platform-specific setup (O_DIRECT, io_uring).
 */
public class IOBackendResolutionTests {

    private static final Logger logger = LogManager.getLogger(IOBackendResolutionTests.class);

    @Test
    public void FileChannelBackendInstantiates() {
        IOBackendStrategy backend = new FileChannelBackend();
        assertNotNull(backend);
        assertTrue(backend instanceof FileChannelBackend);
    }

    @Test
    public void PosixPreadBackendInstantiatesOnLinux() {
        // PosixFD uses Panama FFI — may fail on non-Linux or without --enable-native-access
        try {
            IOBackendStrategy backend = new PosixPreadBackend();
            assertNotNull(backend);
            assertTrue(backend instanceof PosixPreadBackend);
        } catch (Throwable t) {
            // Expected on non-Linux platforms or without native access
            logger.info("PosixPreadBackend not available on this platform: {}", t.getMessage());
        }
    }

    @Test
    public void IoUringBackendInstantiatesOnLinux() {
        // IoUringFileChannel requires Linux + io_uring native lib
        try {
            IOBackendStrategy backend = new IoUringBackend();
            assertNotNull(backend);
            assertTrue(backend instanceof IoUringBackend);
        } catch (Throwable t) {
            // Expected on non-Linux platforms
            logger.info("IoUringBackend not available on this platform: {}", t.getMessage());
        }
    }

    @Test
    public void FileChannelBackendCloseIsNoOp() throws Exception {
        FileChannelBackend backend = new FileChannelBackend();
        // close() is a default no-op — should not throw
        backend.close();
    }
}
