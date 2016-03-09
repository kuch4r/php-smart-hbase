<?php
/**
 * Created by PhpStorm.
 * User: pawel
 * Date: 23.02.2016
 * Time: 12:35
 */

namespace kuchar\smarthbase;
use Hbase\TScan;
use Hbase\TRowResult;


class SmartHTable
{
    protected $table;
    protected $connection;

    public function __construct( $table , SmartHConnection $connection ) {
        $this->table = $table;
        $this->connection = $connection;
    }

    public function row( $key, $columns = array(), $timestamp = null ) {
    	if(is_null($columns)){
    		$columns = array();
    	}
        if( !is_null($timestamp)) {
            $data = $this->connection->nativeGetRowWithColumnsTs($this->table, $key, $columns, $timestamp, array());
        } else {
            $data = $this->connection->nativeGetRowWithColumns($this->table, $key, $columns, array());
        }
        if( !count($data) ) {
            return null;
        }
        return $this->hydrateRow($data[0]);
    }

    public function rows( $keys, $columns = array(), $timestamp = null ) {
    	if(is_null($columns)){
    		$columns = array();
    	}
        if( !is_null($timestamp)) {
            $data = $this->connection->nativeGetRowsWithColumnsTs($this->table, $keys, $columns, $timestamp, array());
        } else {
            $data = $this->connection->nativeGetRowsWithColumns($this->table, $keys, $columns, array());
        }
        return $this->hydrateRows( $data );
    }

    public function scan( $row_start = null, $row_stop = null, $row_prefix = null, $columns = null,
                          $filter = null, $batch_size = 1000,
                          $limit = null, $sorted_columns = false, $scan_batching = null ) {
        if( $batch_size < 1 ){
            throw new \InvalidArgumentException("'batch_size' must be >= 1");
        }
        if( !is_null($limit) && $limit < 1 ) {
            throw new \InvalidArgumentException("'limit' must be >= 1");
        }
        if( !is_null($scan_batching) && $scan_batching < 1 ) {
            throw new \InvalidArgumentException("'scan_batching' must be >= 1");
        }
        if( !is_null($row_prefix)) {
            if( !is_null($row_start) || !is_null($row_stop)) {
                throw new \InvalidArgumentException("'row_prefix' cannot be combined with 'row_start' or 'row_stop'");
            }
            $row_start = $row_prefix;
            $row_stop = $this->str_increment($row_prefix);
        }
        if( is_null($row_start)) {
            $row_start = '';
        }

        $scan = new TScan( array(
            'startRow' => $row_start,
            'stopRow'  => $row_stop,
            'columns'  => $columns,
            'filterString' => $filter,
            'batchSize' => $scan_batching,
            'caching'   => $batch_size,
            'sortColumns' => $sorted_columns
        ));

        $scan_id = $this->connection->nativeScannerOpenWithScan( $this->table, $scan, array() );

        $num_returend = $num_fetched = 0;
        try {
            while (true) {
                if (is_null($limit)) {
                    $how_many = $batch_size;
                } else {
                    $how_many = min($batch_size, $limit - $num_returend);
                }

                $items = $this->connection->nativeScannerGetList($scan_id, $how_many);

                if (empty($items)) {
                    return; // scan has finished
                }

                $num_fetched += count($items);

                foreach ($items as $item) {
                    $row = $this->hydrateRow($item);
                    yield $item->row => $row;

                    $num_returend++;
                    if( !is_null($limit) && $num_returend >= $limit ) {
                        return;
                    }
                }

            }
        } finally {
            $this->connection->nativeScannerClose( $scan_id );
        }
    }

    protected  function hydrateRows( array $result ) {
        $return = array();
        foreach( $result as $row ) {
            $return[$row->row] = $this->hydrateRow($row);
        }
        return $return;
    }

    protected function hydrateRow( TRowResult $result ) {
        $return = array();
        foreach( $result->columns as $column_name => $value ) {
            $return[$column_name] = $value->value;
        }
        return $return;
    }

    protected function str_increment( $str ) {
        for( $i = strlen($str)-1 ; $i >= 0 ; $i-- ) {
            if( ord($str[$i]) != 255 ) {
                return substr($str,0,$i).chr(ord($str[$i])+1);
            }
        }
        return null;
    }
}
