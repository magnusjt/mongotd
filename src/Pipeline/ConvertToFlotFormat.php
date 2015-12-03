<?php namespace Mongotd\Pipeline;

/**
 * Helper pipe for converting a series
 * into the format used by the flot plotting library
 */
class ConvertToFlotFormat{
    public $padding;

    public function __construct($padding=false){
        $this->padding = $padding;
    }

    public function run($input){
        if(is_array($input)){
            $output = [];
            foreach($input as $series){
                $output[] = $this->convertToFlotFormat($series);
            }

            return $output;
        }

        return $this->convertToFlotFormat($input);
    }

    public function convertToFlotFormat(Series $series){
        $data = array_map(function($timestamp, $value){
            if($value === $this->padding){
                // Flot requires 'null' to be the value if no value is present
                $value = null;
            }
            return array($timestamp*1000, $value);
        }, array_keys($series->vals), $series->vals);

        return $data;
    }
}