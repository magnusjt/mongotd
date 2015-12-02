<?php

use Mongotd\Anomaly;
use Mongotd\CounterValue;
use Mongotd\Resolution;

class AddAnomalyStateTest extends PHPUnit_Framework_TestCase{
    public function test_AddAnomalyState_AddsRolledUpStateArray(){
        $start = new DateTime('2015-02-20 00:00:00');
        $end = new DateTime('2015-02-20 00:25:00');
        $resolution = Resolution::FIVE_MINUTES;
        $anomalies = array(
            new Anomaly(new CounterValue('1', '2', new DateTime('2015-02-20 00:01:23'), 1), 1),
            new Anomaly(new CounterValue('1', '2', new DateTime('2015-02-20 00:03:23'), 1), 1),
            new Anomaly(new CounterValue('1', '2', new DateTime('2015-02-20 00:06:15'), 1), 1),
            new Anomaly(new CounterValue('1', '2', new DateTime('2015-02-20 00:07:01'), 1), 1),
            new Anomaly(new CounterValue('1', '2', new DateTime('2015-02-20 00:09:01'), 1), 1),
            new Anomaly(new CounterValue('1', '2', new DateTime('2015-02-20 00:15:01'), 1), 1),
            new Anomaly(new CounterValue('1', '2', new DateTime('2015-02-20 00:16:00'), 1), 1),
            new Anomaly(new CounterValue('1', '2', new DateTime('2015-02-20 00:19:00'), 1), 1),
            new Anomaly(new CounterValue('1', '2', new DateTime('2015-02-20 00:20:00'), 1), 1),
            new Anomaly(new CounterValue('1', '2', new DateTime('2015-02-20 00:24:59'), 1), 1),
        );
        $expectedStates = array(
            (new DateTime('2015-02-20 00:00:00'))->getTimestamp() => 1,
            (new DateTime('2015-02-20 00:05:00'))->getTimestamp() => 1,
            (new DateTime('2015-02-20 00:10:00'))->getTimestamp() => 0,
            (new DateTime('2015-02-20 00:15:00'))->getTimestamp() => 1,
            (new DateTime('2015-02-20 00:20:00'))->getTimestamp() => 1
        );
        $anomalyList = [
            [
                'sid' => 1,
                'nid' => 2,
                'count' => count($anomalies),
                'anomalies' => $anomalies
            ]
        ];
        $expected = [
            [
                'sid' => 1,
                'nid' => 2,
                'count' => count($anomalies),
                'anomalies' => $anomalies,
                'state' => $expectedStates
            ]
        ];

        $action = new \Mongotd\Pipeline\AddAnomalyState($start, $end, $resolution);

        $output = $action->run($anomalyList);

        $this->assertEquals($expected, $output);
    }

}