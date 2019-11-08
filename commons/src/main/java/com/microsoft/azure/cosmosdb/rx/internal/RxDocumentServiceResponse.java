/*
 * The MIT License (MIT)
 * Copyright (c) 2018 Microsoft Corporation
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.microsoft.azure.cosmosdb.rx.internal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.microsoft.azure.cosmosdb.*;
import com.microsoft.azure.cosmosdb.internal.Constants;
import com.microsoft.azure.cosmosdb.internal.HttpConstants;
import com.microsoft.azure.cosmosdb.internal.PathsHelper;
import com.microsoft.azure.cosmosdb.internal.Utils;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.Address;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.StoreResponse;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is core Transport/Connection agnostic response for the Azure Cosmos DB database service.
 */
public class RxDocumentServiceResponse {
    private final int statusCode;
    private final Map<String, String> headersMap;
    private final StoreResponse storeResponse;

    public RxDocumentServiceResponse(StoreResponse response) {
        String[] headerNames = response.getResponseHeaderNames();
        String[] headerValues = response.getResponseHeaderValues();

        this.headersMap = new HashMap<>(headerNames.length);

        // Gets status code.
        this.statusCode = response.getStatus();

        // Extracts headers.
        for (int i = 0; i < headerNames.length; i++) {
            this.headersMap.put(headerNames[i], headerValues[i]);
        }

        this.storeResponse = response;
    }

    public static <T extends Resource> String getResourceKey(Class<T> c) {
        if (c.equals(Attachment.class)) {
            return InternalConstants.ResourceKeys.ATTACHMENTS;
        } else if (c.equals(Conflict.class)) {
            return InternalConstants.ResourceKeys.CONFLICTS;
        } else if (c.equals(Database.class)) {
            return InternalConstants.ResourceKeys.DATABASES;
        } else if (Document.class.isAssignableFrom(c)) {
            return InternalConstants.ResourceKeys.DOCUMENTS;
        } else if (c.equals(DocumentCollection.class)) {
            return InternalConstants.ResourceKeys.DOCUMENT_COLLECTIONS;
        } else if (c.equals(Offer.class)) {
            return InternalConstants.ResourceKeys.OFFERS;
        } else if (c.equals(Permission.class)) {
            return InternalConstants.ResourceKeys.PERMISSIONS;
        } else if (c.equals(Trigger.class)) {
            return InternalConstants.ResourceKeys.TRIGGERS;
        } else if (c.equals(StoredProcedure.class)) {
            return InternalConstants.ResourceKeys.STOREDPROCEDURES;
        } else if (c.equals(User.class)) {
            return InternalConstants.ResourceKeys.USERS;
        } else if (c.equals(UserDefinedFunction.class)) {
            return InternalConstants.ResourceKeys.USER_DEFINED_FUNCTIONS;
        } else if (c.equals(Address.class)) {
            return InternalConstants.ResourceKeys.ADDRESSES;
        } else if (c.equals(PartitionKeyRange.class)) {
            return InternalConstants.ResourceKeys.PARTITION_KEY_RANGES;
        }

        throw new IllegalArgumentException("c");
    }

    public int getStatusCode() {
        return this.statusCode;
    }

    public Map<String, String> getResponseHeaders() {
        return this.headersMap;
    }

    public String getReponseBodyAsString() {
        return this.storeResponse.getResponseBody();
    }

    public <T extends Resource> T getResource(Class<T> c) {
        T resource;
        // Response body ObjectNode may be available directly if we know that it's a JSON resource
        ObjectNode objectNode = storeResponse.getResponseObjectNode();
        if (objectNode != null) {
            try {
                resource = c.getConstructor(ObjectNode.class).newInstance(objectNode);
            } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
                    | NoSuchMethodException | SecurityException e) {
                throw new IllegalStateException("Failed to instantiate class object.", e);
            }
        } else {
            // Otherwise, assume response body is a string
            String responseBody = storeResponse.getResponseBody();
            if (StringUtils.isEmpty(responseBody)) {
                return null;
            }
            try {
                resource = c.getConstructor(String.class).newInstance(responseBody);
            } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
                    | NoSuchMethodException | SecurityException e) {
                throw new IllegalStateException("Failed to instantiate class object.", e);
            }
        }
        if (PathsHelper.isPublicResource(resource)) {
            BridgeInternal.setAltLink(resource, PathsHelper.generatePathForNameBased(resource, this.getOwnerFullName(),resource.getId()));
        }

        return resource;
    }

    private JsonNode getBodyJsonNode() {
        ObjectNode json = storeResponse.getResponseObjectNode();
        if (json != null) {
            return json;
        }
        String body = storeResponse.getResponseBody();
        return body != null ? fromJson(body) : null;
    }

    public <T extends Resource> List<T> getQueryResponse(Class<T> c) {
        JsonNode json = getBodyJsonNode();
        if (json == null) {
            return new ArrayList<>();
        }

        String resourceKey = RxDocumentServiceResponse.getResourceKey(c);
        ArrayNode jTokenArray = (ArrayNode) json.get(resourceKey);

        // Aggregate queries may return a nested array
        ArrayNode innerArray;
        while (jTokenArray != null && jTokenArray.size() == 1 && (innerArray = toArrayNode(jTokenArray.get(0))) != null) {
            jTokenArray = innerArray;
        }

        List<T> queryResults = new ArrayList<>();

        if (jTokenArray != null) {
            jTokenArray.forEach(jToken -> {
                // Aggregate on single partition collection may return the aggregated value only,
                // in which case it needs to encapsulated in an object node.
                ObjectNode resourceJson;
                if (jToken.isNumber() || jToken.isBoolean()) {
                    resourceJson = Utils.getSimpleObjectMapper().createObjectNode()
                        .put(Constants.Properties.AGGREGATE, jToken.asText());
                } else if (jToken.isObject()) {
                    resourceJson = (ObjectNode) jToken;
                } else {
                    throw new IllegalStateException("Array element not an object node");
                }
                try {
                    queryResults.add(c.getConstructor(ObjectNode.class).newInstance(resourceJson));
                } catch (Exception e) {
                    throw new IllegalStateException("Failed to instantiate class object", e);
                }
            });
        }

        return queryResults;
    }

    private ArrayNode toArrayNode(JsonNode n) {
        if (n.isArray()) {
            return (ArrayNode) n;
        } else {
            return null;
        }
    }

    private static JsonNode fromJson(String json) {
        try {
            return Utils.getSimpleObjectMapper().readTree(json);
        } catch (IOException e) {
            throw new IllegalStateException(String.format("Unable to parse JSON %s", json), e);
        }
    }

    private static String toJson(Object object){
        try {
            return Utils.getSimpleObjectMapper().writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Can't serialize the object into the json string", e);
        }
    }

    private String getOwnerFullName() {
        if (this.headersMap != null) {
            return this.headersMap.get(HttpConstants.HttpHeaders.OWNER_FULL_NAME);
        }
        return null;
    }

    public InputStream getContentStream() {
        return this.storeResponse.getResponseStream();
    }

    public ClientSideRequestStatistics getClientSideRequestStatistics() {
        if (this.storeResponse == null) {
            return null;
        }
        return this.storeResponse.getClientSideRequestStatistics();
    }
}
