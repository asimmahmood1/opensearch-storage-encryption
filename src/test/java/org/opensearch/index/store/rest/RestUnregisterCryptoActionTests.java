/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.opensearch.rest.RestRequest.Method.POST;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

public class RestUnregisterCryptoActionTests {

    private RestUnregisterCryptoAction action;
    private RestRequest request;
    private NodeClient client;

    @Before
    public void setUp() throws Exception {
        action = new RestUnregisterCryptoAction();
        request = mock(RestRequest.class);
        client = mock(NodeClient.class);
    }

    @Test
    public void GetName() {
        assertEquals("unregister_key_action", action.getName());
    }

    @Test
    public void Routes() {
        assertEquals(1, action.routes().size());
        assertEquals(POST, action.routes().get(0).getMethod());
        assertEquals("/_plugins/_opensearch_storage_encryption/_unregister_key", action.routes().get(0).getPath());
    }

    @Test
    public void PrepareRequestThrowsUnsupportedOperationException() {
        assertThrows(UnsupportedOperationException.class, () -> action.prepareRequest(request, client));
    }
}
