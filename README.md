# PHP HBase Lib

This lib is based on HBase Thrift and python happybase package

## Usage

```$php
use kuchar\smarthbase\SmartHConnection;

$uri = "example.hbase.host.com:9090/?protocol=binary&transport=framed"
$connection = new SmartHConnection($uri);

$table = $connection->table('test');

# put one row
$table->put('row_id', ['cf:column_name' => 'value']);

# read one row
$table->row('row_id');

# put many rows row
$table->put('row_id1', ['cf:column_name' => 'value1']);
$table->put('row_id2', ['cf:column_name' => 'value2']);


# read multiple rows
$table->rows(['row_id1', 'row_id2']);

# scan table
$items = []
foreach( $table->scan() as $row => $vals) {
            $items[$row] = $vals;
}

# delete whole row
$table->delete('row_id1');

# delete column from row
$table->delete('row_id2', ['cf:column_name']);

# batch operations
$batch = new SmartHBatch($table);
$batch->put('row_id1', ['cf:column_name' => 'value1']);
$batch->put('row_id2', ['cf:column_name' => 'value2']);
$batch->send();

```

## Testing

```$bash
# copy default config template and set missing parameters
cp tests/phpunit.xml.dist tests/phpunit.xml

# run tests
./vendor/bin/phpunit -c tests/phpunit.xml tests
```