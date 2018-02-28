<?php

namespace kuchar\Tests\smarthbase;

use PHPUnit\Framework\TestCase;
use kuchar\smarthbase\SmartHConnection;

class SmartHConnectionTest extends TestCase {
    public function testConnectionListOfUris() {
        foreach( explode(',', THRIFT_CONNECT_URIS) as $uri) {
            $connection = new SmartHConnection($uri);
            $this->assertInstanceOf('kuchar\smarthbase\SmartHConnection', $connection);
            $this->assertTrue($connection->isOpen());
            $connection->close();
        }
    }

    public function testConnection() {
        $connection = new SmartHConnection(THRIFT_TEST_URI);
        $this->assertInstanceOf('kuchar\smarthbase\SmartHConnection', $connection);
        $this->assertTrue($connection->isOpen());
        $connection->close();
        $this->assertFalse($connection->isOpen());
    }

    public function testNativeListTables()
    {
        $connection = new SmartHConnection(THRIFT_TEST_URI);
        $result = $connection->nativeGetTableNames();

        $this->assertContains(THRIFT_TEST_TABLE, $result);
        $connection->close();
    }
}