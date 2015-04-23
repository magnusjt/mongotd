<?php namespace Mongotd;

class Anomaly{
    /** @var  CounterValue */
    public $cv;

    /** @var  number */
    public $predicted;

    public function __construct(CounterValue $cv, $predicted){
        $this->cv = $cv;
        $this->predicted = $predicted;
    }
}