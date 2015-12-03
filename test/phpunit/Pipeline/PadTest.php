<?php

use Mongotd\Pipeline\ConvertToDateStringKeys;
use Mongotd\Pipeline\Pad;
use Mongotd\Pipeline\Series;
use Mongotd\Resolution;

class PadTest extends PHPUnit_Framework_TestCase{
    public function test_FiveMinResolutionDailySum_RollupTime_ReturnSumForEachDay(){
        $padding = false;
        $start = new DateTime('2015-10-25 00:00:00');
        $end = new DateTime('2015-10-25 06:00:00');
        $resolution = Resolution::HOUR;
        $series = new Series([
            (new DateTime('2015-10-25 00:00:00'))->getTimestamp() => 1,
            (new DateTime('2015-10-25 02:00:00'))->getTimestamp() => 1,
            (new DateTime('2015-10-25 04:00:00'))->getTimestamp() => 1,
            (new DateTime('2015-10-25 05:00:00'))->getTimestamp() => 1,
        ]);
        $expected = [
            '2015-10-25 00:00:00' => 1,
            '2015-10-25 01:00:00' => $padding,
            '2015-10-25 02:00:00' => 1,
            '2015-10-25 03:00:00' => $padding,
            '2015-10-25 04:00:00' => 1,
            '2015-10-25 05:00:00' => 1,
        ];

        $convert = new ConvertToDateStringKeys();
        $action = new Pad($resolution, $start, $end, $padding);
        $output = $action->run($series);
        $output = $convert->run($output);

        $this->assertEquals($expected, $output);
    }
}