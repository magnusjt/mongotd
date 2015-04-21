<?php
namespace Mongotd;

use \DateTime;
use \DateInterval;
use \DatePeriod;

class SignalGenerator{
    function generateSineDailyPeriodWithNoise($nDays, $interval = 300, $noiseRate = 0.1){
        $datetimeEnd = new DateTime('now');
        $datetimeStart = clone $datetimeEnd;
        $datetimeStart->sub(DateInterval::createFromDateString($nDays . ' days'));
        $dateperiod = new DatePeriod($datetimeStart, DateInterval::createFromDateString($interval . ' seconds'), $datetimeEnd);

        $series = array();
        $amplitude = 100;
        /** @var \DateTime $datetime */
        foreach($dateperiod as $datetime){
            $signal = $amplitude/2 + ($amplitude/2)*sin(M_PI*2*($datetime->getTimestamp()%86400)/86400);
            $signal += $amplitude*$noiseRate*(rand()%1000)/1000; // Random value between -1 and 1, times noise amplitude
            $series[] = array('datetime' => $datetime, 'value' => $signal);
        }

        return $series;
    }
}