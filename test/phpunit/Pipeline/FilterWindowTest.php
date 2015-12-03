<?php

use Mongotd\Pipeline\ConvertToDateStringKeys;
use Mongotd\Pipeline\FilterWindow;
use Mongotd\Pipeline\Series;

class FilterWindowTest extends PHPUnit_Framework_TestCase{
    public function test_FourDailyWindowsWithSpacingEqualToResolution_FilterWindow_ReturnsFourValues(){
        $valSpacing = 300;
        $windowLength = 300;
        $windowDistance = 86400;
        $value = 1;
        $expected = [
            '2015-10-24 02:00:00' => $value,
            '2015-10-25 02:00:00' => $value,
            '2015-10-26 02:00:00' => $value,
            '2015-10-27 02:00:00' => $value
        ];
        $start = new DateTime('2015-10-24 02:00:00');
        $end = new DateTime('2015-10-27 02:05:00');
        $dateperiod = new DatePeriod($start, DateInterval::createFromDateString($valSpacing.' seconds'), $end);
        $valsByTimestamp = [];
        foreach($dateperiod as $datetime){
            $valsByTimestamp[$datetime->getTimestamp()] = $value;
        }
        $series = new Series($valsByTimestamp);

        $action = new FilterWindow($start, $windowLength, $windowDistance);
        $output = $action->run($series);

        $convert = new ConvertToDateStringKeys();
        $output = $convert->run($output);

        $this->assertEquals($expected, $output);
    }

    public function test_RandomInput(){
        $start = new DateTime('2015-10-23 23:00:00');
        $windowLength = 60*60*3; // 3 hours
        $windowDistance = 86400;
        $value = 1;
        $input = [
            '2015-10-24 00:00:00' => $value,
            '2015-10-24 01:47:00' => $value,
            '2015-10-24 02:00:00' => $value,
            '2015-10-25 00:32:00' => $value,
            '2015-10-25 01:45:00' => $value,
            '2015-10-25 02:07:00' => $value,
            '2015-10-25 03:05:00' => $value,
            '2015-10-26 01:15:00' => $value,
            '2015-10-26 02:26:00' => $value,
            '2015-10-26 03:10:00' => $value,
            '2015-10-26 08:10:00' => $value,
            '2015-10-26 23:00:00' => $value,
            '2015-10-26 23:59:59' => $value,
        ];
        $expected = [
            '2015-10-24 00:00:00' => $value,
            '2015-10-24 01:47:00' => $value,
            '2015-10-25 00:32:00' => $value,
            '2015-10-25 01:45:00' => $value,
            '2015-10-26 01:15:00' => $value,
            '2015-10-26 23:00:00' => $value,
            '2015-10-26 23:59:59' => $value,
        ];

        $vals = [];
        foreach($input as $dateStr => $val){
            $vals[(new DateTime($dateStr))->getTimestamp()] = $val;
        }
        $series = new Series($vals);

        $action = new FilterWindow($start, $windowLength, $windowDistance);
        $output = $action->run($series);

        $convert = new ConvertToDateStringKeys();
        $output = $convert->run($output);

        $this->assertEquals($expected, $output);
    }
}