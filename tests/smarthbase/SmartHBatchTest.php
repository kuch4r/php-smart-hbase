<?php

namespace kuchar\Tests\smarthbase;

use PHPUnit\Framework\TestCase;
use kuchar\smarthbase\SmartHConnection;
use kuchar\smarthbase\SmartHTable;
use kuchar\smarthbase\SmartHBatch;
use \ReflectionClass;

class SmartHBatchTest extends TestCase {
    protected $connection;

    public function setUp(){
        $this->connection = new SmartHConnection(THRIFT_TEST_URI);
    }

    public function tearDown()
    {
        $this->connection->close();
    }

    public function testEmptyBatch()
    {
        $table = new SmartHTable(THRIFT_TEST_TABLE, $this->connection);
        $batch = new SmartHBatch($table);
        $this->assertEquals(0, $batch->send());
    }

    public function testPut()
    {
        $table = new SmartHTable(THRIFT_TEST_TABLE, $this->connection);
        $batch = new SmartHBatch($table);
        $batch_ref = new ReflectionClass($batch);

        $batch->put('row1', [THRIFT_TEST_CF.':col1' => 'val1']);

        $prop = $batch_ref->getProperty('mutation_count');
        $prop->setAccessible(true);
        $this->assertEquals(1, $prop->getValue($batch), "Mutation count is not 1");

        $prop = $batch_ref->getProperty('mutations');
        $prop->setAccessible(true);
        $this->assertCount(1, $prop->getValue($batch));

        $method = $batch_ref->getMethod('resetMutations');
        $method->setAccessible(true);
        $method->invoke($batch);

        $prop = $batch_ref->getProperty('mutation_count');
        $prop->setAccessible(true);
        $this->assertEquals(0, $prop->getValue($batch), "Mutation count is not 0");

        $prop = $batch_ref->getProperty('mutations');
        $prop->setAccessible(true);
        $this->assertCount(0, $prop->getValue($batch));
    }

    public function testAddRows() {
        $table = new SmartHTable(THRIFT_TEST_TABLE, $this->connection);

        $table->delete('row1');
        $table->delete('row2');

        $batch = new SmartHBatch($table);
        $batch->put('row1', [THRIFT_TEST_CF.':col1' => 'val1']);
        $batch->put('row2', [THRIFT_TEST_CF.':col1' => 'val1']);

        $result = $table->row('row1');
        $this->assertNull($result);

        $batch->send();

        $result = $table->rows(['row1', 'row2']);

        $this->assertCount(2, $result);
        $this->assertArrayHasKey('row1', $result);
        $this->assertArrayHasKey('row2', $result);
    }

    public function testDeleteColumn() {
        $table = new SmartHTable(THRIFT_TEST_TABLE, $this->connection);

        $table->delete('row1');
        $table->delete('row2');

        $batch = new SmartHBatch($table);
        $batch->put('row1', [THRIFT_TEST_CF.':col1' => 'val1', THRIFT_TEST_CF.':col2' => 'val2']);
        $batch->put('row2', [THRIFT_TEST_CF.':col1' => 'val1', THRIFT_TEST_CF.':col3' => 'val3']);
        $batch->send();

        $result = $table->rows(['row1', 'row2']);

        $this->assertCount(2, $result);
        $this->assertArrayHasKey(THRIFT_TEST_CF.':col2', $result['row1']);
        $this->assertArrayHasKey(THRIFT_TEST_CF.':col3', $result['row2']);

        $batch = new SmartHBatch($table);
        $batch->delete('row1', [THRIFT_TEST_CF.':col2']);
        $batch->delete('row2', [THRIFT_TEST_CF.':col3']);
        $batch->send();

        $result = $table->rows(['row1', 'row2']);

        $this->assertCount(2, $result);
        $this->assertArrayNotHasKey(THRIFT_TEST_CF.':col2', $result['row1']);
        $this->assertArrayNotHasKey(THRIFT_TEST_CF.':col3', $result['row2']);
    }
}

