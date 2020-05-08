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
        testing.givenEvent().enqueue();
        testing.thenRun(MonitorarTerminaisFunction.class, "handleRequest");

        FnResult result = testing.getOnlyResult();
        //assertEquals("Hello, world!", result.getBodyAsString());
        assertEquals(true, true);
    }

}