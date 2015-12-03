<?php namespace Mongotd\StorageMiddleware;

use Mongotd\CounterValue;

/**
 * This class makes sure that every counter value
 * is in fact a number, and converts the values to
 * actual ints or floats.
 *
 * In addition, it casts nid/sid to string to make
 * sure we store them in a consistent way.
 */
class FilterCounterValues{
    /**
     * @param $cvs CounterValue[]
     *
     * @return CounterValue[]
     */
    public function run(array $cvs){
        $filtered = [];

        foreach($cvs as $cv){
            if(!is_numeric($cv->value)){
                continue;
            }

            if(is_string($cv->value)){
                if(ctype_digit($cv->value)){
                    $cv->value = (int)$cv->value;
                }else{
                    $cv->value = (float)$cv->value;
                }
            }

            $cv->sid = (string)$cv->sid;
            $cv->nid = (string)$cv->nid;
            $filtered[] = $cv;
        }

        return $filtered;

    }
}