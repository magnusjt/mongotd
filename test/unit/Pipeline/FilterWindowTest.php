<?php

use Mongotd\Pipeline\ConvertToDateStringKeys;
use Mongotd\Pipeline\FilterWindow;
use Mongotd\Pipeline\Series;

class FilterWindowTest extends PHPUnit_Framework_TestCase{
    public function test_FourDailyWindowsWithSpacingEqualToResolution_FilterWindow_ReturnsFourValues(){
        $totalLength = 86400*3;
        $valSpacing = 300;
        $windowLength = 300;
        $windowDistance = 86400;
        $value = 1;
        $start = new DateTime('2015-10-25 02:00:00');
        $end = clone $start;
        $end->add(DateInterval::createFromDateString(($totalLength+$windowLength).' seconds'));
        $expected = [
            '2015-10-25 02:00:00' => $value,
            '2015-10-26 02:00:00' => $value,
            '2015-10-27 02:00:00' => $value,
            '2015-10-28 02:00:00' => $value
        ];

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

}