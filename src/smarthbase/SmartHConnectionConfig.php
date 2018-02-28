<?php

namespace kuchar\smarthbase;

class SmartHConnectionConfig {
    public $uri;
    public $host;
    public $port;
    public $transport = 'framed';
    public $protocol = 'binary';
    public $sendTimeout = 10000;
    public $recvTimeout = 20000;

    public function __construct($uri)
    {
        $this->uri = $uri;
        $this->parseUri();
    }

    protected function parseUri()
    {
        $parts = parse_url($this->uri);
        if (!isset($parts['host']) || !isset($parts['port'])) {
            throw new \InvalidArgumentException('Wrong uri, you have to provide host & port');
        }
        $this->host = $parts['host'];
        $this->port = $parts['port'];

        if (isset($parts['query'])) {
            $this->parseQueryString($parts['query']);
        }
    }

    protected function parseQueryString($query) {
        parse_str($query, $vars);
        foreach($vars as $name => $val) {
            $this->parseParam($name, $val);
        }
    }

    protected function parseParam($name, $value) {
        switch($name) {
            case 'protocol':
                if( !in_array($value, ['binary', 'compact'], false)) {
                    throw new \InvalidArgumentException('Uknown protocol type, use binary or compact');
                }
                $this->protocol = $value;
                break;
            case 'transport':
                if( !in_array($value, ['framed', 'buffered'], false)) {
                    throw new \InvalidArgumentException('Uknown protocol type, use framed or buffered');
                }
                $this->transport = $value;
                break;
            case 'timeout':
                $value = (int) $value;
                if($value < 1000) {
                    throw new \InvalidArgumentException('Timeout has to be larger then 1000 (1s)');
                }
                $this->sendTimeout = $this->recvTimeout = $value;
                break;
        }
    }
}