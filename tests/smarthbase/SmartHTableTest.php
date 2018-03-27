<?php

namespace kuchar\Tests\smarthbase;

use PHPUnit\Framework\TestCase;
use kuchar\smarthbase\SmartHConnection;
use kuchar\smarthbase\SmartHTable;

class SmartHTableTest extends TestCase {
    protected $connection;

    public function setUp(){
        $this->connection = new SmartHConnection(THRIFT_TEST_URI);
    }

    public function tearDown()
    {
        $this->connection->close();
    }

    public function testAddRow() {
        $table = new SmartHTable(THRIFT_TEST_TABLE, $this->connection);
        $table->put('row1', [THRIFT_TEST_CF.':col1' => 'val1']);
        $result = $table->row('row1');

        $this->assertArrayHasKey(THRIFT_TEST_CF.':col1', $result);
        $this->assertEquals('val1', $result[THRIFT_TEST_CF.':col1']);
    }

    public function testAddRowFromConnection() {
        $table = $this->connection->table(THRIFT_TEST_TABLE);
        $table->put('row2', [THRIFT_TEST_CF.':col2' => 'val2']);
        $result = $table->row('row2');

        $this->assertArrayHasKey(THRIFT_TEST_CF.':col2', $result);
        $this->assertEquals('val2', $result[THRIFT_TEST_CF.':col2']);
    }

    public function testAdd2RowsFromConnection() {
        $table = $this->connection->table(THRIFT_TEST_TABLE);
        $table->put('row1', [THRIFT_TEST_CF.':col2' => 'val2']);
        $table->put('row2', [THRIFT_TEST_CF.':col2' => 'val3']);
        $result = $table->rows(['row1', 'row2']);

        $this->assertEquals(2, count($result));
        $this->assertArrayHasKey('row1', $result);
        $this->assertArrayHasKey('row2', $result);
        $this->assertArrayHasKey(THRIFT_TEST_CF.':col2', $result['row1']);
        $this->assertArrayHasKey(THRIFT_TEST_CF.':col2', $result['row2']);
        $this->assertEquals('val2', $result['row1'][THRIFT_TEST_CF.':col2']);
    }

    public function testScan() {
        $table = $this->connection->table(THRIFT_TEST_TABLE);
        $table->put('row1', [THRIFT_TEST_CF.':col1' => 'val1']);
        $table->put('row2', [THRIFT_TEST_CF.':col2' => 'val2']);
        $items = [];
        foreach( $table->scan() as $row => $vals) {
            $items[$row] = $vals;
        }

        $this->assertArrayHasKey('row1', $items);
        $this->assertArrayHasKey(THRIFT_TEST_CF.':col1', $items['row1']);
        $this->assertEquals('val1', $items['row1'][THRIFT_TEST_CF.':col1']);
        $this->assertArrayHasKey('row2', $items);
    }
}