<?php

use Mongotd\Aggregation;
use Mongotd\Pipeline\ConvertToDateStringKeys;
use Mongotd\Pipeline\RollupTime;
use Mongotd\Pipeline\Series;
use Mongotd\Resolution;

class RollupTimeTest extends PHPUnit_Framework_TestCase{
    public function test_FiveMinResolutionDailySum_RollupTime_ReturnSumForEachDay(){
        $value = 1;
        $nDays = 20;
        $resolution = Resolution::DAY;
        $aggregation = Aggregation::SUM;
        $start = new DateTIme('2015-10-10 00:00:00');
        $end = clone $start;
        $end->add(DateInterval::createFromDateString($nDays.' days'));

        $dateperiodExpected = new DatePeriod($start, DateInterval::createFromDateString('1 day'), $end);
        $expected = [];
        foreach($dateperiodExpected as $datetime){
            $expected[$datetime->format('Y-m-d H:i:s')] = $value*(86400/$resolution);
        }

        $dateperiod = new DatePeriod($start, DateInterval::createFromDateString($resolution.' seconds'), $end);
        $valsByTimestamp = [];
        foreach($dateperiod as $datetime){
            $valsByTimestamp[$datetime->getTimestamp()] = $value;
        }
        $series = new Series($valsByTimestamp);
        $convert = new ConvertToDateStringKeys();

        $action = new RollupTime($resolution, $aggregation);
        $output = $action->run($series);
        $output = $convert->run($output);

        $this->assertEquals($expected, $output);
    }
}