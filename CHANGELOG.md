# CHANGELOG

## 2.1.0

#### fixed
- Compact protocol is using not existing class TCompactProtocolAccelerated

#### changes
- Added new protocol value 'binary_accelerated' for use of TBinaryProtocolAccelerated. <br>
'compact' and 'binary' protocols use standard PHP protocol classes
(Current version of Thrift don't support the compact accelerated protocol for PHP).
- SmartHTable 'delete' method now deletes whole row if you don't provide columns list
  (It's not a breaking change since in previous version columns parameters was always required).
- Added tests for SmartHBatch

## 2.0.0

#### breaking changes
- using uri instead of host & port in SmartHConnection

```
example.hbase.host.com:9090/?protocol=binary&transport=framed&timeout=20000
```

#### new features
- supports protocol setting by uri option
- supports transport setting by uri option

#### changes
- basic set of tests

## 1.0.0

Initial release