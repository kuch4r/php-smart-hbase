<?php

namespace kuchar\Tests\smarthbase;

use PHPUnit\Framework\TestCase;
use kuchar\smarthbase\SmartHConnectionConfig;

class SmartHConnectionConfigTest extends TestCase {

    public function testSimpleUri() {
        $config = new SmartHConnectionConfig("test.host.com:9090");
        $this->assertEquals('test.host.com', $config->host);
        $this->assertEquals(9090, $config->port);
    }

    /**
     * @expectedException InvalidArgumentException
     * @expectedExceptionMessage Wrong uri, you have to provide host & port
     */
    public function testMissingPort() {
        new SmartHConnectionConfig("test.host.com");
    }

    /**
     * @expectedException InvalidArgumentException
     * @expectedExceptionMessage Wrong uri, you have to provide host & port
     */
    public function testEmptyUri() {
        new SmartHConnectionConfig("");
    }

    public function testArgsBinaryFramed() {
        $config = new SmartHConnectionConfig("test.host.com:9090/?protocol=binary&transport=framed&timeout=9999");
        $this->assertEquals('test.host.com', $config->host);
        $this->assertEquals(9090, $config->port);
        $this->assertEquals('binary', $config->protocol);
        $this->assertEquals('framed', $config->transport);
        $this->assertEquals('9999', $config->recvTimeout);
        $this->assertEquals('9999', $config->sendTimeout);
    }

    public function testArgsCompactBuffered() {
        $config = new SmartHConnectionConfig("test.host.com:9090/?protocol=compact&transport=buffered");
        $this->assertEquals('test.host.com', $config->host);
        $this->assertEquals(9090, $config->port);
        $this->assertEquals('compact', $config->protocol);
        $this->assertEquals('buffered', $config->transport);
    }

    public function testArgsBinaryAccelerated() {
        $config = new SmartHConnectionConfig("test.host.com:9090/?protocol=binary_accelerated&transport=buffered");
        $this->assertEquals('test.host.com', $config->host);
        $this->assertEquals(9090, $config->port);
        $this->assertEquals('binary_accelerated', $config->protocol);
        $this->assertEquals('buffered', $config->transport);
    }

    /**
     * @expectedException InvalidArgumentException
     * @expectedExceptionMessage Uknown protocol type, use binary or compact
     */
    public function testWrongProtocol() {
        new SmartHConnectionConfig("test.host.com:9090/?protocol=wrong");
    }

    /**
     * @expectedException InvalidArgumentException
     * @expectedExceptionMessage Timeout has to be larger then 1000 (1s)
     */
    public function testWrongTimeout() {
        new SmartHConnectionConfig("test.host.com:9090/?timeout=1");
    }
}