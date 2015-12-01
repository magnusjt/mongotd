<?php namespace Mongotd\Pipeline;

class Series{
    /** @var  array */
    public $vals;

    public function __construct($vals){
        $this->vals = $vals;
    }
}