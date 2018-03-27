<?php
/**
 * Created by PhpStorm.
 * User: pawel
 * Date: 23.02.2016
 * Time: 12:35
 */

namespace kuchar\smarthbase;
use Hbase\BatchMutation;
use Hbase\Mutation;

class SmartHBatch {
    protected $table;
    protected $mutations;
    protected $mutation_count;
    protected $batch_size;
    public function __construct( $table, $batch_size = 0 ) {
        $this->table = $table;
        $this->batch_size = $batch_size;
        $this->resetMutations();
    }

    protected function resetMutations() {
        $this->mutations = array();
        $this->mutation_count = 0;
    }

    public function put( $row, $data ) {
        if( !isset($this->mutations[$row])) {
            $this->mutations[$row] = array();
        }
        foreach( $data as $column => $value ) {
            $this->mutations[$row][] = new Mutation( array('column' => $column, 'value' => $value, 'isDelete' => false) );
        }
        $this->mutation_count += count($data);
        if( $this->batch_size && $this->mutation_count > $this->batch_size ) {
            $this->send();
        }
    }

    public function delete( $row, $columns) {
        if( !isset($this->mutations[$row])) {
            $this->mutations[$row] = array();
        }

        foreach ($columns as $column) {
            $this->mutations[$row][] = new Mutation(array('column' => $column, 'isDelete' => true));
        }

        $this->mutation_count += count($columns);
        if( $this->batch_size && $this->mutation_count > $this->batch_size ) {
            $this->send();
        }
    }

    public function send() {
        $bms = array();
        foreach( $this->mutations as $row => $m ) {
            $bms[] = new BatchMutation(array('row' => $row, 'mutations' => $m));
        }
        if( empty($bms)) {
            return 0;
        }
        $this->table->getConnection()->nativeMutateRows( $this->table->getTable(), $bms, array() );

        $this->resetMutations();
        return count($bms);
    }
}