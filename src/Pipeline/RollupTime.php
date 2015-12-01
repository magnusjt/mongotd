<?php namespace Mongotd\Pipeline;

use Mongotd\Aggregation;

class RollupTime{
    public $resolution;

    public $aggregation;

    public $padding = false;

    public function __construct($resolution, $aggregation, $padding = false){
        $this->resolution = $resolution;
        $this->aggregation = $aggregation;
        $this->padding = $padding;
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
                $output[] = $this->rollup($series);
            }
            return $output;
        }

        return $this->rollup($input);
    }

    public function rollup(Series $series){
        $vals = array();
        $nVals = array();
        foreach($series->vals as $timestamp => $value){
            if($value === $this->padding){
                continue;
            }

            /*
             * Here we find the unix time offset by the the timezone
             * We clamp that time down to the desired resolution, and
             * lastly move the clamped time back to unix time.
             *
             * NB: Timezone offset may be different at the rounded time
             *     because of daylight savings time
             */
            $timezoneOffset = date('Z', $timestamp);
            $timezoneTime = $timestamp + $timezoneOffset;
            $roundedTime = $timezoneTime - ($timezoneTime % $this->resolution);
            $timezoneOffset = date('Z', $roundedTime);
            $roundedTime -= $timezoneOffset;

            if($this->aggregation == Aggregation::SUM){
                isset($vals[$roundedTime]) ?
                    $vals[$roundedTime] += $value :
                    $vals[$roundedTime] = $value;
            }elseif($this->aggregation == Aggregation::AVG){
                isset($vals[$roundedTime]) ?
                    $vals[$roundedTime] += $value :
                    $vals[$roundedTime] = $value;
                isset($nVals[$roundedTime]) ?
                    $nVals[$roundedTime]++ :
                    $nVals[$roundedTime] = 1;
            }elseif($this->aggregation == Aggregation::MAX){
                isset($vals[$roundedTime]) ?
                    $vals[$roundedTime] = max($vals[$roundedTime], $value) :
                    $vals[$roundedTime] = $value;
            }elseif($this->aggregation == Aggregation::MIN){
                isset($vals[$roundedTime]) ?
                    $vals[$roundedTime] = min($vals[$roundedTime], $value) :
                    $vals[$roundedTime] = $value;
            }
        }

        if($this->aggregation == Aggregation::AVG){
            foreach($vals as $second => $value){
                $vals[$second] = $value/$nVals[$second];
            }
        }

        return new Series($vals);
    }
}