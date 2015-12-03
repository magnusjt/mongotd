<?php namespace Mongotd\Pipeline;

use Mongotd\Aggregation;

/**
 * This pipe takes a list of series, and merges them using the specified aggregation.
 * It requires that the timestamp keys in each subseries are the same,
 * so each subseries should be rolled up in time and padded first.
 */
class RollupSpace{
    public $aggregation;

    public $padding = false;

    public function __construct($aggregation, $padding = false){
        $this->aggregation = $aggregation;
        $this->padding = $padding;
    }

    /**
     * @param $input Series|Series[]
     *
     * @return Series
     */
    public function run($input){
        if(!is_array($input)){
            return $input;
        }

        return $this->rollup($input);
    }

    /**
     * @param $seriesList Series[]
     *
     * @return Series
     */
    public function rollup($seriesList){
        $vals = array();
        $nVals = array();

        foreach($seriesList as $series){
            foreach($series->vals as $timestamp => $value){
                if($value === $this->padding){
                    continue;
                }

                if($this->aggregation == Aggregation::SUM){
                    isset($vals[$timestamp]) ?
                        $vals[$timestamp] += $value :
                        $vals[$timestamp] = $value;
                }elseif($this->aggregation == Aggregation::AVG){
                    isset($vals[$timestamp]) ?
                        $vals[$timestamp] += $value :
                        $vals[$timestamp] = $value;
                    isset($nVals[$timestamp]) ?
                        $nVals[$timestamp]++ :
                        $nVals[$timestamp] = 1;
                }elseif($this->aggregation == Aggregation::MAX){
                    isset($vals[$timestamp]) ?
                        $vals[$timestamp] = max($vals[$timestamp], $value) :
                        $vals[$timestamp] = $value;
                }elseif($this->aggregation == Aggregation::MIN){
                    isset($vals[$timestamp]) ?
                        $vals[$timestamp] = min($vals[$timestamp], $value) :
                        $vals[$timestamp] = $value;
                }
            }
        }

        if($this->aggregation == Aggregation::AVG){
            foreach($vals as $timestamp => $value){
                $vals[$timestamp] /= $nVals[$timestamp];
            }
        }

        return new Series($vals);
    }
}