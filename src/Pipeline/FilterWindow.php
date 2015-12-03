<?php namespace Mongotd\Pipeline;

use DateInterval;
use DateTime;

class FilterWindow{
    public $start;
    public $windowLength;
    public $distance;
    public $dateIntervalDistance;
    public $dateIntervalWindowLength;

    /**
     * @param $start        DateTime First window
     * @param $windowLength int Window length in seconds
     * @param $distance     int Distance between windows
     */
    public function __construct(DateTime $start, $windowLength, $distance){
        $this->start = $start;
        $this->windowLength = $windowLength;
        $this->distance = $distance;
        $this->dateIntervalDistance = DateInterval::createFromDateString($this->distance.' seconds');
        $this->dateIntervalWindowLength = DateInterval::createFromDateString($this->windowLength.' seconds');
    }

    public function run(Series $series){
        $vals = [];
        $start = clone $this->start;
        $end = clone $this->start;
        $end->add($this->dateIntervalWindowLength);
        $tsStart = $start->getTimestamp();
        $tsEnd = $end->getTimestamp();

        foreach($series->vals as $timestamp => $value){
            // Step 1. Move input forward if it's before the current window
            if($timestamp < $tsStart){
                continue;
            }

            // Step 2. Move window forward if it's before the current input
            while($timestamp >= $tsEnd){
                $start->add($this->dateIntervalDistance);
                $end->add($this->dateIntervalDistance);
                $tsStart = $start->getTimestamp();
                $tsEnd = $end->getTimestamp();
            }

            if($timestamp >= $tsStart){
                $vals[$timestamp] = $value;
            }
        }

        return new Series($vals);
    }
}