<?php

namespace kuchar\phphbase;

/** Thrift root directory */
require 'Thrift/ClassLoader/ThriftClassLoader.php';


use Thrift\ClassLoader\ThriftClassLoader;
$loader = new ThriftClassLoader();
$loader->registerNamespace('Thrift', __DIR__);
$loader->registerDefinition('Hbase', 'HBase');
$loader->register(true);


use Hbase\HbaseClient;
use Thrift\Protocol\TBinaryProtocolAccelerated;
use Thrift\Transport\TSocket;
use Thrift\Transport\TFramedTransport;
use Thrift\Exception\TException;

/**
 * HBase Client class
 */
class PHPHBaseConnection
{
    protected $hbase_host;
    protected $hbase_port;

    protected $socket;
    protected $transport;
    protected $protocol;
    protected $client;

    public function __construct( $host, $port )
    {
        $this->hbase_host = $host;
        $this->hbase_port = $port;
        $this->socket    = new TSocket( $this->hbase_host, $this->hbase_port, true );
        $this->socket->setSendTimeout(120,600);
        $this->socket->setRecvTimeout(240,1500);
        $this->transport = new TFramedTransport( $this->socket );
        $this->protocol  = new TBinaryProtocolAccelerated( $this->transport );
        $this->client    = new HbaseClient( $this->protocol );


        if(!$this->transport->isOpen()){
            try{
                $this->transport->open();
            } catch(TException $e){
                $this->socket->close();
            }
        }
    }

    public function __destruct()
    {
        $this->transport->close();
        $this->socket->close();
    }

    public function getClient() {
        return $this->client;
    }
}
