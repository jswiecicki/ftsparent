package com.CSBFTS.PerformanceAnalysis

class ConsumerRunnerTest {//extends GroovyTestCase {

    void testRunConsumer() {

    }

    void testElasticConnection() {
        assertEquals(true, ConsumerRunner.elasticConnection());
    }
}
