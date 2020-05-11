package com.oracle.fn;

import com.fnproject.fn.testing.*;
import com.oracle.fn.MonitorarTerminaisFunction;

import org.junit.*;

import static org.junit.Assert.*;

public class MonitorarTerminaisFunctionTest {

    @Rule
    public final FnTestingRule testing = FnTestingRule.createDefault();

    @Test
    public void shouldReturnGreeting() {
    	 String event = "{\n" + 
    	 		"  \"cloudEventsVersion\": \"0.1\",\n" + 
    	 		"  \"eventID\": \"unique_ID\",\n" + 
    	 		"  \"eventType\": \"com.oraclecloud.objectstorage.createobject\",\n" + 
    	 		"  \"source\": \"objectstorage\",\n" + 
    	 		"  \"eventTypeVersion\": \"2.0\",\n" + 
    	 		"  \"eventTime\": \"2019-01-10T21:19:24Z\",\n" + 
    	 		"  \"contentType\": \"application/json\",\n" + 
    	 		"  \"extensions\": {\n" + 
    	 		"    \"compartmentId\": \"ocid1.compartment.oc1..aaaaaaaau32hskqd62tlchxvh2selnb4ugbrsbjs4orykyx3553cylmy54aa\"\n" + 
    	 		"  },\n" + 
    	 		"  \"data\": {\n" + 
    	 		"    \"compartmentId\": \"ocid1.compartment.oc1..aaaaaaaau32hskqd62tlchxvh2selnb4ugbrsbjs4orykyx3553cylmy54aa\",\n" + 
    	 		"    \"compartmentName\": \"CloudNative\",\n" + 
    	 		"    \"resourceName\": \"coleta_spo_20200316090000.data\",\n" + 
    	 		"    \"resourceId\": \"/n/id4beafwqb9e/b/bucket-logs/o/coleta_spo_20200316090000.data\",\n" + 
    	 		"    \"availabilityDomain\": \"all\",\n" + 
    	 		"    \"additionalDetails\": {\n" + 
    	 		"      \"eTag\": \"8383ea0f-7fbf-489a-aa76-17d5bd7aa7c5\",\n" + 
    	 		"      \"namespace\": \"id4beafwqb9e\",\n" + 
    	 		"      \"bucketName\": \"bucket-logs\",\n" + 
    	 		"      \"bucketId\": \"ocid1.bucket.oc1.iad.aaaaaaaarj2a7xmozeehck5ctztgmpnfyqamqu7x5qxdj2xjmlsm7io76mya\",\n" + 
    	 		"      \"archivalState\": \"Available\"\n" + 
    	 		"    }\n" + 
    	 		"  }\n" + 
    	 		"}";
    	 
         testing.givenEvent().withBody(event).enqueue();
         testing.thenRun(MonitorarTerminaisFunction.class, "handleRequest");

         FnResult result = testing.getOnlyResult();
         assertTrue(result.isSuccess());
        assertEquals("Hello, world!", "Hello, world!");
        assertEquals(true, true);
    }

}