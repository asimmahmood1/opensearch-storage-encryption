/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import java.util.Collections;
import java.util.Map;

import org.opensearch.common.Randomness;
import org.opensearch.common.crypto.DataKeyPair;
import org.opensearch.common.crypto.MasterKeyProvider;

/**
 * Utility class providing a dummy MasterKeyProvider implementation for testing.
 * This is used by yamlRestTests and integration tests to avoid requiring
 * a real KMS plugin during testing.
 *
 * <p><b>WARNING:</b> This is for testing purposes only and should never be
 * used in production environments. The dummy provider:
 * <ul>
 * <li>Generates random keys without actual encryption</li>
 * <li>Returns encrypted keys as-is during decryption (no actual decryption)</li>
 * <li>Provides no real security</li>
 * </ul>
 *
 * @opensearch.internal
 */
public final class DummyKeyProvider {

    /**
     * Private constructor to prevent instantiation of utility class.
     */
    private DummyKeyProvider() {
        throw new AssertionError("Utility class should not be instantiated");
    }

    /**
     * Creates a dummy MasterKeyProvider for testing purposes.
     * This provider generates random keys and returns encrypted keys as-is.
     *
     * @return a mock MasterKeyProvider suitable for testing
     */
    public static MasterKeyProvider create() {
        return new MasterKeyProvider() {
            // Static keys for testing - 32 bytes of predictable data
            private static final byte[] STATIC_RAW_KEY = {
                    0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
                    0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
                    0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
                    0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20
            };

            private static final byte[] STATIC_ENCRYPTED_KEY = {
                    0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
                    0x29, 0x2A, 0x2B, 0x2C, 0x2D, 0x2E, 0x2F, 0x30,
                    0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38,
                    0x39, 0x3A, 0x3B, 0x3C, 0x3D, 0x3E, 0x3F, 0x40
            };

            @Override
            public DataKeyPair generateDataPair() {
                return new DataKeyPair(STATIC_RAW_KEY.clone(), STATIC_ENCRYPTED_KEY.clone());
            }

            @Override
            public byte[] decryptKey(byte[] encryptedKey) {
                // For mock/testing purposes, just return the input as-is
                return STATIC_RAW_KEY.clone();
            }

            @Override
            public String getKeyId() {
                return "static-test-key-id";
            }

            @Override
            public Map<String, String> getEncryptionContext() {
                return Collections.emptyMap();
            }

            @Override
            public void close() {
                // Nothing to close for mock implementation
            }
        };
    }
}

