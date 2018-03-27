<?php

namespace kuchar\smarthbase;

use Hbase\HbaseClient;
use Thrift\Protocol\TBinaryProtocol;
use Thrift\Protocol\TBinaryProtocolAccelerated;
use Thrift\Protocol\TCompactProtocol;
use Thrift\Transport\TBufferedTransport;
use Thrift\Transport\TSocket;
use Thrift\Transport\TFramedTransport;
use Thrift\Exception\TException;



/**
 * Hbase Client class
 */
class SmartHConnection
{
    protected $connectionConfig;

    protected $socket;
    protected $transport;
    protected $client;

    protected $retryNum = 2;

    public static $NO_RETRY_FUNCTIONS = array('increment','incrementRows','atomicIncrement');


    public function __construct( $uri )
    {
        $this->connectionConfig = new SmartHConnectionConfig($uri);
        $this->connect();
    }

    public function close() {
        if($this->transport === null || !$this->transport->isOpen()){
            return;
        }
        $this->transport->close();
        $this->transport = null;
        $this->socket->close();
    }

    public function isOpen() {
        return $this->transport !== null && $this->transport->isOpen();
    }

    public function table( $name ) {
        return new SmartHTable( $name , $this );
    }

    public function __destruct() {
        $this->close();
    }

    public function setRetryCount( $num ) {
        $num = (int) $num;
        if ($num < 1 ) {
            throw new \InvalidArgumentException("Retry count value cannot be less then 1");
        }
        $this->retryNum = $num;
    }

    public function getRetryCount() {
        return $this->retryNum;
    }

    /**
     * Expose native client methods with "native" prefix, e.x. "nativeGetRow()"
     *
     * @param $name
     * @param $arguments
     * @return mixed
     * @throws TTransportException
     */
    public function __call($name, $arguments) {
        if( 0 !== strpos($name, 'native')){
            throw new \RuntimeException("Unknown method: ".$name);
        }
        $callname = lcfirst(substr($name,6));
        if( !method_exists($this->client,$callname)) {
            throw new \RuntimeException("Unknown method: ".$name);
        }

        $retryNum = in_array($name, self::$NO_RETRY_FUNCTIONS) ? 1 : $this->retryNum;

        for( $num = 0 ; $num < $retryNum ; $num++ ) {
            try {
                return call_user_func_array(array($this->client,$callname),$arguments);
            } catch (TTransportException $e) {}
        }
    }

    protected function connect()
    {
        $this->socket = new TSocket($this->connectionConfig->host, $this->connectionConfig->port, true);
        $this->socket->setSendTimeout($this->connectionConfig->sendTimeout);
        $this->socket->setRecvTimeout($this->connectionConfig->recvTimeout);

        if ($this->connectionConfig->transport == 'framed') {
            $this->transport = new TFramedTransport($this->socket);
        } else {
            $this->transport = new TBufferedTransport($this->socket);
        }

        if ($this->connectionConfig->protocol == 'binary_accelerated') {
            $this->protocol = new TBinaryProtocolAccelerated($this->transport);
        } elseif($this->connectionConfig->protocol == 'binary') {
            $this->protocol  = new TBinaryProtocol( $this->transport );
        } else {
            $this->protocol  = new TCompactProtocol( $this->transport );
        }

        $this->client    = new HbaseClient( $this->protocol );

        if(!$this->transport->isOpen()){
            try{
                $this->transport->open();
            } catch(TException $e){
                $this->socket->close();
            }
        }

    }
}
