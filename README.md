# PHP HBase Lib

This lib is based on HBase Thrift and python happybase package

## Usage

```$php
use kuchar\smarthbase\SmartHConnection;

$uri = "example.hbase.host.com:9090/?protocol=binary&transport=framed"
$connection = new SmartHConnection($uri);

$table = $connection->table('test');

$table->put('row_id', ['cf:column_name' => 'value']);
$table->row('row_id');
```

