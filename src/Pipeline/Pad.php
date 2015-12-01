<?php namespace Mongotd\Pipeline;

use DateTime;

class Pad{
    public $resolution;
    public $start;
    public $end;
    public $padding = false;
    public $dateperiod;

    public function __construct($resolution, DateTime $start, DateTime $end, $padding = false){
        $this->resolution = $resolution;
        $this->start = $start;
        $this->end = $end;
        $this->padding = $padding;
        $this->dateperiod = new \DatePeriod($start, \DateInterval::createFromDateString($resolution.' seconds'), $end);
    }

    /**
     * @param $input Series|Series[]
     *
     * @return Series|Series[]
     */
    public function run($input){
        if(is_array($input)){
            $output = [];
            foreach($input as $series){
                $output[] = $this->pad($series);
            }

            return $output;
        }

        return $this->pad($input);
    }

    public function pad(Series $series){
        $vals = array();
        foreach($this->dateperiod as $datetime){
            $vals[$datetime->getTimestamp()] = $this->padding;
        }

        foreach($series->vals as $second => $value){
            if(isset($vals[$second])){
                $vals[$second] = $value;
            }
        }

        return new Series($vals);
    }
}