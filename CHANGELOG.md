# CHANGELOG

## 2.0.0

### breaking changes
- using uri instead of host & port in SmartHConnection

```
example.hbase.host.com:9090/?protocol=binary&transport=framed&timeout=20000
```

### new features
- supports protocol setting by uri option
- supports transport setting by uri option

### changes
- basic set of tests

## 1.0.0

Initial release