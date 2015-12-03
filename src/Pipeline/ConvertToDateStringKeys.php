<?php namespace Mongotd\Pipeline;

use DateTime;
use DateTimeZone;

/**
 * Helper pipe for converting the timestamps
 * in a series to datetime strings.
 * It makes testing a lot easier.
 */
class ConvertToDateStringKeys{
    public $datetimeZone;

    public function __construct(){
        $this->datetimeZone = new DateTimeZone(date_default_timezone_get());
    }

    public function run($input){
        if(is_array($input)){
            $output = [];
            foreach($input as $series){
                $output[] = $this->convertToDateStringKeys($series);
            }

            return $output;
        }

        return $this->convertToDateStringKeys($input);
    }

    public function convertToDateStringKeys(Series $series){
        $valsByDateStr = array();
        foreach($series->vals as $timestamp => $value){
            $datetime = new DateTime('@'.($timestamp));
            $datetime->setTimezone($this->datetimeZone);
            $valsByDateStr[$datetime->format('Y-m-d H:i:s')] = $value;
        }

        return $valsByDateStr;
    }
}