<?php namespace Mongotd\Pipeline;

class FilterWindow{
    public $start;

    public $windowLength;

    public $distance;

    public function __construct($start, $windowLength, $distance){
        $this->start = $start;
        $this->windowLength = $windowLength;
        $this->distance = $distance;
    }

    public function run(Series $series){
        $vals = [];
        $start = $this->start;
        $end = $start + $this->windowLength;
        foreach($series->vals as $timestamp => $value){
            if($timestamp > $end){
                $start += $this->distance;
                $end += $this->distance;
            }

            if($timestamp >= $start){
                $vals[$timestamp] = $value;
            }
        }

        return new Series($vals);
    }
}