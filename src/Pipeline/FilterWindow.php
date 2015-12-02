<?php namespace Mongotd\Pipeline;

use DateInterval;
use DateTime;

class FilterWindow{
    public $start;
    public $windowLength;
    public $distance;

    /**
     * @param $start        DateTime First window
     * @param $windowLength int Window length in seconds
     * @param $distance     int Distance between windows
     */
    public function __construct(DateTime $start, $windowLength, $distance){
        $this->start = $start;
        $this->windowLength = $windowLength;
        $this->distance = $distance;
    }

    public function run(Series $series){
        $vals = [];
        $distanceInterval = DateInterval::createFromDateString($this->distance.' seconds');
        $windowInterval = DateInterval::createFromDateString($this->windowLength.' seconds');
        $start = clone $this->start;
        $end = clone $start;
        $end->add($windowInterval);
        $tsStart = $start->getTimestamp();
        $tsEnd = $end->getTimestamp();

        foreach($series->vals as $timestamp => $value){
            if($timestamp >= $tsEnd){
                $start->add($distanceInterval);
                $end->add($distanceInterval);
                $tsStart = $start->getTimestamp();
                $tsEnd = $end->getTimestamp();
            }

            if($timestamp >= $tsStart && $timestamp < $tsEnd){
                $vals[$timestamp] = $value;
            }
        }

        return new Series($vals);
    }
}