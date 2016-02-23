<?php

namespace kuchar\phphbase;

/** Thrift root directory */
require 'Thrift/ClassLoader/ThriftClassLoader.php';


use Thrift\ClassLoader\ThriftClassLoader;
$loader = new ThriftClassLoader();
$loader->registerNamespace('Thrift', __DIR__);
$loader->registerNamespace('Hbase', __DIR__);
$loader->register(true);


use Hbase\HbaseClient;
use Hbase\HbaseIf;
use Thrift\Protocol\TBinaryProtocol;
use Thrift\Transport\TSocket;
use Thrift\Transport\TFramedTransport;
use Thrift\Exception\TException;

/**
 * HBase Client class
 */
class SmartHbaseClient
{
    protected $hbase_host;
    protected $hbase_port;

    protected $socket;
    protected $transport;
    protected $protocol;
    protected $client;

    protected $retryNum = 2;

    protected const NO_RETRY_FUNCTIONS = array('increment','incrementRows','atomicIncrement');

    public function __construct( $host, $port )
    {
        $this->hbase_host = $host;
        $this->hbase_port = $port;
        $this->socket    = new TSocket( $this->hbase_host, $this->hbase_port, true );
        $this->socket->setSendTimeout(120,600);
        $this->socket->setRecvTimeout(240,1500);
        $this->transport = new TFramedTransport( $this->socket );
        $this->protocol  = new TBinaryProtocol( $this->transport );
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

    public function setRetryCount( $num ) {
        $this->retryNum = $num;
    }

    public function getRetryCount() {
        return $this->retryNum;
    }

    public function __call($name, $arguments)
    {
        $retryNum = in_array($name, self::NO_RETRY_FUNCTIONS) ? 1 : $this->retryNum;
        for( $num = 0 ; $num < $retryNum ; $num++ ) {
            try {
                return call_user_func_array(array($this->client,$name),$arguments);
            } catch (TTransportException $e) {}
        }
        throw $e;
    }
}
