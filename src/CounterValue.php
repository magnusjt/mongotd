<?php
namespace Mongotd;

use DateTime;

class CounterValue{
    /** @var  string */
    public $sid;

    /** @var  string */
    public $nid;

    /** @var DateTime DateTime */
    public $datetime;

    /** @var  number */
    public $value;

    /** @var  boolean */
    public $incremental;

    public function __construct($sid, $nid, DateTime $datetime, $value, $incremental = false){
        $this->sid = $sid;
        $this->nid = $nid;
        $this->datetime = $datetime;
        $this->value = $value;
        $this->incremental = $incremental;
    }
}