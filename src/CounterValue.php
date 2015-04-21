<?php
namespace Mongotd;

use \DateTime;

class CounterValue{
    public $sid;
    public $nid;
    public $datetime;
    public $value;

    public function __construct($sid, $nid, DateTime $datetime, $value){
        $this->sid = $sid;
        $this->nid = $nid;
        $this->datetime = $datetime;
        $this->value = $value;
    }
}