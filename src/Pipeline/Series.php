<?php namespace Mongotd\Pipeline;

class Series{
    /**
     * An array with keys = unix timestamp, and values = values
     *
     * @var  array
     */
    public $vals;

    public function __construct($vals){
        $this->vals = $vals;
    }
}