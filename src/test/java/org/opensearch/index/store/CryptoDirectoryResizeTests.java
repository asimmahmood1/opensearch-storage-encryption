/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.Provider;
import java.security.Security;

import javax.crypto.spec.SecretKeySpec;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.niofs.CryptoNIOFSDirectory;

/**
 * Tests for clone/resize operations in CryptoDirectoryFactory.
 * Verifies that keyfiles are correctly copied from source to target indices
 * during clone, split, and shrink operations.
 */
public class CryptoDirectoryResizeTests {

    private Path tempDir;
    private CryptoDirectoryFactory factory;

    @Before
    public void setUp() throws Exception {
        tempDir = Files.createTempDirectory("crypto-resize-test");
        factory = new CryptoDirectoryFactory();
    }

    @After
    public void tearDown() throws Exception {
        if (tempDir != null && Files.exists(tempDir)) {
            Files.walk(tempDir).sorted((a, b) -> -a.compareTo(b)).forEach(p -> {
                try { Files.deleteIfExists(p); } catch (IOException e) { /* ignore */ }
            });
        }
    }

    private void invokeHandleResizeKeyfileCopy(Settings settings, String indexName, Path targetIndexDirectory) throws Exception {
        factory.handleResizeOperation(settings, indexName, targetIndexDirectory);
    }

    /**
     * Helper method to create mock Settings with resize metadata.
     */
    private Settings createResizeSettings(String sourceUuid, String sourceName) {
        return Settings.builder()
            .put("index.resize.source.uuid", sourceUuid)
            .put("index.resize.source.name", sourceName)
            .build();
    }

    /**
     * Test that keyfile is copied from source to target during clone operation.
     */
    @Test
    public void CloneOperationCopiesKeyfile() throws Exception {
        String sourceUuid = "source-uuid-123";
        String targetUuid = "target-uuid-456";

        Path indicesDir = tempDir.resolve("indices");
        Files.createDirectories(indicesDir);

        Path sourceIndexDir = indicesDir.resolve(sourceUuid);
        Files.createDirectories(sourceIndexDir);

        Path targetIndexDir = indicesDir.resolve(targetUuid);
        Files.createDirectories(targetIndexDir);

        // Create source keyfile with test data
        byte[] sourceKeyData = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        try (FSDirectory dir = FSDirectory.open(sourceIndexDir)) {
            try (IndexOutput out = dir.createOutput("keyfile", IOContext.DEFAULT)) {
                out.writeInt(sourceKeyData.length);
                out.writeBytes(sourceKeyData, 0, sourceKeyData.length);
            }
        }

        Path sourceKeyfile = sourceIndexDir.resolve("keyfile");
        assertTrue("Source keyfile should exist", Files.exists(sourceKeyfile));

        // Create Settings with clone metadata
        Settings settings = createResizeSettings(sourceUuid, "source-index");

        // Invoke the private method
        invokeHandleResizeKeyfileCopy(settings, "test-index", targetIndexDir);

        // Verify keyfile was copied to target
        Path targetKeyfile = targetIndexDir.resolve("keyfile");
        assertTrue("Target keyfile should exist after clone", Files.exists(targetKeyfile));

        // Verify keyfile contents are identical
        byte[] targetKeyData;
        try (FSDirectory dir = FSDirectory.open(targetIndexDir)) {
            try (org.apache.lucene.store.IndexInput in = dir.openInput("keyfile", IOContext.READONCE)) {
                int length = in.readInt();
                targetKeyData = new byte[length];
                in.readBytes(targetKeyData, 0, length);
            }
        }

        assertArrayEquals("Keyfile contents should match", sourceKeyData, targetKeyData);
    }

    /**
     * Test that split operation copies keyfile from source to target.
     */
    @Test
    public void SplitOperationCopiesKeyfile() throws Exception {
        String sourceUuid = "split-source-uuid";
        String targetUuid = "split-target-uuid";

        Path indicesDir = tempDir.resolve("indices");
        Files.createDirectories(indicesDir);

        Path sourceIndexDir = indicesDir.resolve(sourceUuid);
        Files.createDirectories(sourceIndexDir);

        Path targetIndexDir = indicesDir.resolve(targetUuid);
        Files.createDirectories(targetIndexDir);

        // Create source keyfile
        byte[] keyData = new byte[] { 9, 8, 7, 6, 5, 4, 3, 2, 1 };
        try (FSDirectory dir = FSDirectory.open(sourceIndexDir)) {
            try (IndexOutput out = dir.createOutput("keyfile", IOContext.DEFAULT)) {
                out.writeInt(keyData.length);
                out.writeBytes(keyData, 0, keyData.length);
            }
        }

        // Create IndexSettings with split metadata
        Settings settings = createResizeSettings(sourceUuid, "split-source");

        // Invoke the private method
        invokeHandleResizeKeyfileCopy(settings, "test-index-01", targetIndexDir);
        Path targetKeyfile = targetIndexDir.resolve("keyfile");
        assertTrue("Target keyfile should exist after split", Files.exists(targetKeyfile));

        byte[] targetKeyData;
        try (FSDirectory dir = FSDirectory.open(targetIndexDir)) {
            try (org.apache.lucene.store.IndexInput in = dir.openInput("keyfile", IOContext.READONCE)) {
                int length = in.readInt();
                targetKeyData = new byte[length];
                in.readBytes(targetKeyData, 0, length);
            }
        }

        assertArrayEquals("Split keyfile contents should match source", keyData, targetKeyData);
    }

    /**
     * Test that shrink operation copies keyfile from source to target.
     */
    @Test
    public void ShrinkOperationCopiesKeyfile() throws Exception {
        String sourceUuid = "shrink-source-uuid";
        String targetUuid = "shrink-target-uuid";

        Path indicesDir = tempDir.resolve("indices");
        Files.createDirectories(indicesDir);

        Path sourceIndexDir = indicesDir.resolve(sourceUuid);
        Files.createDirectories(sourceIndexDir);

        Path targetIndexDir = indicesDir.resolve(targetUuid);
        Files.createDirectories(targetIndexDir);

        // Create source keyfile
        byte[] keyData = new byte[32];
        for (int i = 0; i < 32; i++) {
            keyData[i] = (byte) i;
        }
        try (FSDirectory dir = FSDirectory.open(sourceIndexDir)) {
            try (IndexOutput out = dir.createOutput("keyfile", IOContext.DEFAULT)) {
                out.writeInt(keyData.length);
                out.writeBytes(keyData, 0, keyData.length);
            }
        }

        // Create IndexSettings with shrink metadata
        Settings settings = createResizeSettings(sourceUuid, "shrink-source");

        // Invoke the private method
        invokeHandleResizeKeyfileCopy(settings, "test-index-02", targetIndexDir);

        Path targetKeyfile = targetIndexDir.resolve("keyfile");
        assertTrue("Target keyfile should exist after shrink", Files.exists(targetKeyfile));

        byte[] targetKeyData;
        try (FSDirectory dir = FSDirectory.open(targetIndexDir)) {
            try (org.apache.lucene.store.IndexInput in = dir.openInput("keyfile", IOContext.READONCE)) {
                int length = in.readInt();
                targetKeyData = new byte[length];
                in.readBytes(targetKeyData, 0, length);
            }
        }

        assertArrayEquals("Shrink keyfile contents should match source", keyData, targetKeyData);
    }

    /**
     * Test that non-resize operations don't copy keyfiles.
     */
    @Test
    public void NonResizeOperationDoesNotCopyKeyfile() throws Exception {
        String indexUuid = "regular-index-uuid";

        Path indicesDir = tempDir.resolve("indices");
        Files.createDirectories(indicesDir);

        Path indexDir = indicesDir.resolve(indexUuid);
        Files.createDirectories(indexDir);

        // Create Settings WITHOUT resize metadata
        Settings settings = Settings.EMPTY;

        Path keyfile = indexDir.resolve("keyfile");
        assertFalse("Keyfile should not exist before operation", Files.exists(keyfile));

        // Invoke the private method - should not copy anything
        invokeHandleResizeKeyfileCopy(settings, "test-index-03", indexDir);

        // Keyfile should still not exist after non-resize operation
        assertFalse("Keyfile should not be created for non-resize operation", Files.exists(keyfile));
    }

    /**
     * Test that missing source keyfile is handled gracefully.
     */
    @Test
    public void MissingSourceKeyfileHandledGracefully() throws Exception {
        String sourceUuid = "missing-source-uuid";
        String targetUuid = "target-uuid";

        Path indicesDir = tempDir.resolve("indices");
        Files.createDirectories(indicesDir);

        // Create source directory but NO keyfile
        Path sourceIndexDir = indicesDir.resolve(sourceUuid);
        Files.createDirectories(sourceIndexDir);

        Path targetIndexDir = indicesDir.resolve(targetUuid);
        Files.createDirectories(targetIndexDir);

        Settings settings = createResizeSettings(sourceUuid, "missing-source");

        // Should not throw - missing source keyfile should be logged and handled
        invokeHandleResizeKeyfileCopy(settings, "test-index-04", targetIndexDir);

        // Verify target keyfile was not created
        Path targetKeyfile = targetIndexDir.resolve("keyfile");
        assertFalse("Target keyfile should not exist when source is missing", Files.exists(targetKeyfile));
    }

    /**
     * Test that existing target keyfile is not overwritten.
     */
    @Test
    public void ExistingTargetKeyfileNotOverwritten() throws Exception {
        String sourceUuid = "source-existing-uuid";
        String targetUuid = "target-existing-uuid";

        Path indicesDir = tempDir.resolve("indices");
        Files.createDirectories(indicesDir);

        Path sourceIndexDir = indicesDir.resolve(sourceUuid);
        Files.createDirectories(sourceIndexDir);

        Path targetIndexDir = indicesDir.resolve(targetUuid);
        Files.createDirectories(targetIndexDir);

        // Create source keyfile
        byte[] sourceKeyData = new byte[] { 1, 2, 3 };
        try (FSDirectory dir = FSDirectory.open(sourceIndexDir)) {
            try (IndexOutput out = dir.createOutput("keyfile", IOContext.DEFAULT)) {
                out.writeInt(sourceKeyData.length);
                out.writeBytes(sourceKeyData, 0, sourceKeyData.length);
            }
        }

        // Create EXISTING target keyfile with different data
        byte[] existingTargetKeyData = new byte[] { 9, 9, 9 };
        try (FSDirectory dir = FSDirectory.open(targetIndexDir)) {
            try (IndexOutput out = dir.createOutput("keyfile", IOContext.DEFAULT)) {
                out.writeInt(existingTargetKeyData.length);
                out.writeBytes(existingTargetKeyData, 0, existingTargetKeyData.length);
            }
        }

        Settings settings = createResizeSettings(sourceUuid, "source-index");

        // Invoke the private method
        invokeHandleResizeKeyfileCopy(settings, "test-index-05" ,targetIndexDir);

        // Verify target keyfile still has original data (not overwritten)
        byte[] targetKeyData;
        try (FSDirectory dir = FSDirectory.open(targetIndexDir)) {
            try (org.apache.lucene.store.IndexInput in = dir.openInput("keyfile", IOContext.READONCE)) {
                int length = in.readInt();
                targetKeyData = new byte[length];
                in.readBytes(targetKeyData, 0, length);
            }
        }

        assertArrayEquals("Existing target keyfile should not be overwritten", existingTargetKeyData, targetKeyData);
    }

    /**
     * End-to-end test: Write encrypted data to source directory, copy keyfile,
     * copy encrypted files, and verify cloned directory can decrypt and read data.
     *
     * This test simulates the actual clone flow:
     * 1. Create source directory and write encrypted files via CryptoNIOFSDirectory
     * 2. Clone operation copies keyfile
     * 3. Clone operation copies encrypted data files
     * 4. Target directory can decrypt and read the same data
     */
    @Test
    public void EndToEndCloneWithEncryptedDocuments() throws Exception {
        String sourceUuid = "source-e2e-uuid";
        String targetUuid = "target-e2e-uuid";

        Path indicesDir = tempDir.resolve("indices");
        Files.createDirectories(indicesDir);

        Path sourceIndexDir = indicesDir.resolve(sourceUuid);
        Files.createDirectories(sourceIndexDir);

        Path targetIndexDir = indicesDir.resolve(targetUuid);
        Files.createDirectories(targetIndexDir);

        // Create a shared encryption key for testing
        byte[] rawKey = new byte[32]; // 256-bit AES key
        java.util.Random rnd = new java.util.Random();
        rnd.nextBytes(rawKey);

        // Create KeyResolver that returns the same key for both source and target
        KeyResolver keyResolver = mock(KeyResolver.class);
        when(keyResolver.getDataKey()).thenReturn(new SecretKeySpec(rawKey, "AES"));

        Provider provider = Security.getProvider("SunJCE");

        // Create keyfile in source directory
        byte[] encryptedKeyData = new byte[32];
        rnd.nextBytes(encryptedKeyData);
        try (FSDirectory dir = FSDirectory.open(sourceIndexDir)) {
            try (IndexOutput out = dir.createOutput("keyfile", IOContext.DEFAULT)) {
                out.writeInt(encryptedKeyData.length);
                out.writeBytes(encryptedKeyData, 0, encryptedKeyData.length);
            }
        }

        // Step 1: Write encrypted data files to source directory via CryptoNIOFSDirectory
        EncryptionMetadataCache sourceCache = new EncryptionMetadataCache();
        Directory sourceDir = new CryptoNIOFSDirectory(FSLockFactory.getDefault(), sourceIndexDir, provider, keyResolver, sourceCache);

        byte[] doc1Data = "test document one".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] doc2Data = "test document two".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] doc3Data = "test document three".getBytes(java.nio.charset.StandardCharsets.UTF_8);

        try (IndexOutput out = sourceDir.createOutput("doc1.dat", IOContext.DEFAULT)) {
            out.writeInt(doc1Data.length);
            out.writeBytes(doc1Data, 0, doc1Data.length);
        }
        try (IndexOutput out = sourceDir.createOutput("doc2.dat", IOContext.DEFAULT)) {
            out.writeInt(doc2Data.length);
            out.writeBytes(doc2Data, 0, doc2Data.length);
        }
        try (IndexOutput out = sourceDir.createOutput("doc3.dat", IOContext.DEFAULT)) {
            out.writeInt(doc3Data.length);
            out.writeBytes(doc3Data, 0, doc3Data.length);
        }

        // Verify source can read the data back
        try (org.apache.lucene.store.IndexInput in = sourceDir.openInput("doc1.dat", IOContext.READONCE)) {
            int len = in.readInt();
            byte[] buf = new byte[len];
            in.readBytes(buf, 0, len);
            assertArrayEquals("Source doc1 should decrypt correctly", doc1Data, buf);
        }

        sourceDir.close();

        // Step 2: Simulate clone operation - copy keyfile
        Settings settings = createResizeSettings(sourceUuid, "source-index");
        invokeHandleResizeKeyfileCopy(settings, "test-index-06", targetIndexDir);

        // Verify keyfile was copied
        Path targetKeyfile = targetIndexDir.resolve("keyfile");
        assertTrue("Target keyfile should exist after clone", Files.exists(targetKeyfile));

        // Step 3: Copy encrypted data files to target (simulating Lucene segment file copy)
        for (String fileName : new String[]{"doc1.dat", "doc2.dat", "doc3.dat"}) {
            Files.copy(sourceIndexDir.resolve(fileName), targetIndexDir.resolve(fileName), StandardCopyOption.REPLACE_EXISTING);
        }

        // Step 4: Open target directory and verify it can decrypt and read the same data
        EncryptionMetadataCache targetCache = new EncryptionMetadataCache();
        Directory targetDir = new CryptoNIOFSDirectory(FSLockFactory.getDefault(), targetIndexDir, provider, keyResolver, targetCache);

        try (org.apache.lucene.store.IndexInput in = targetDir.openInput("doc1.dat", IOContext.READONCE)) {
            int len = in.readInt();
            byte[] buf = new byte[len];
            in.readBytes(buf, 0, len);
            assertArrayEquals("Cloned doc1 should decrypt to same content", doc1Data, buf);
        }
        try (org.apache.lucene.store.IndexInput in = targetDir.openInput("doc2.dat", IOContext.READONCE)) {
            int len = in.readInt();
            byte[] buf = new byte[len];
            in.readBytes(buf, 0, len);
            assertArrayEquals("Cloned doc2 should decrypt to same content", doc2Data, buf);
        }
        try (org.apache.lucene.store.IndexInput in = targetDir.openInput("doc3.dat", IOContext.READONCE)) {
            int len = in.readInt();
            byte[] buf = new byte[len];
            in.readBytes(buf, 0, len);
            assertArrayEquals("Cloned doc3 should decrypt to same content", doc3Data, buf);
        }

        targetDir.close();
    }
}
