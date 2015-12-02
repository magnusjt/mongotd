<?php

use Mongotd\Aggregation;
use Mongotd\Pipeline\ConvertToDateStringKeys;
use Mongotd\Pipeline\RollupSpace;
use Mongotd\Pipeline\Series;

class RollupSpaceTest extends PHPUnit_Framework_TestCase{
    public function test_FiveMinResolutionSumTwoSeries_RollupSpace_ReturnSumForBothSeries(){
        $value1 = 10;
        $value2 = 20;
        $nDays = 20;
        $aggregation = Aggregation::SUM;
        $start = new DateTIme('2015-10-10 00:00:00');
        $end = clone $start;
        $end->add(DateInterval::createFromDateString($nDays.' days'));
        $dateperiod = new DatePeriod($start, DateInterval::createFromDateString('5 minutes'), $end);


        $vals1 = [];
        $vals2 = [];
        $expected = [];
        foreach($dateperiod as $datetime){
            $vals1[$datetime->getTimestamp()] = $value1;
            $vals2[$datetime->getTimestamp()] = $value2;
            $expected[$datetime->format('Y-m-d H:i:s')] = $value1 + $value2;
        }

        $series1 = new Series($vals1);
        $series2 = new Series($vals2);

        $convert = new ConvertToDateStringKeys();
        $action = new RollupSpace($aggregation);

        $output = $action->run([$series1, $series2]);
        $output = $convert->run($output);

        $this->assertEquals($expected, $output);
    }
}