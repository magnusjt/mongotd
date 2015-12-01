<?php
namespace Mongotd;

use DateInterval;
use DateTime;
use InvalidArgumentException;

class DateTimeHelper{
    /**
     * @param $datetime \DateTime
     * @return \DateTime
     */
    static public function clampToMonth($datetime){
        $datetime = clone $datetime;
        $datetime->setDate($datetime->format('Y'), $datetime->format('m'), 1);
        return $datetime;
    }

    /**
     * @param $datetime \DateTime
     * @return \DateTime
     */
    static public function clampToWeek($datetime){
        $datetime = new DateTime($datetime->format('o-\WW-1')); // 1 is monday. o is year taking into account week number 53 and such.
        $datetime->setTime(0, 0, 0);
        return $datetime;
    }

    /**
     * @param $datetime \DateTime
     * @return \DateTime
     */
    static public function clampToDay($datetime){
        $datetime = clone $datetime;
        $datetime->setTime(0, 0, 0);
        return $datetime;
    }

    /**
     * @param $datetime \DateTime
     * @return \DateTime
     */
    static public function clampToHour($datetime){
        $datetime = clone $datetime;
        $datetime->setTime($datetime->format('H'), 0, 0);
        return $datetime;
    }

    /**
     * @param $datetime \DateTime
     * @return \DateTime
     */
    static public function clampToFifteenMin($datetime){
        $datetime = clone $datetime;
        $minutes = (int)$datetime->format('i');
        $datetime->setTime($datetime->format('H'), $minutes-$minutes%15, 0);
        return $datetime;
    }

    /**
     * @param $datetime \DateTime
     * @return \DateTime
     */
    static public function clampToFiveMin($datetime){
        $datetime = clone $datetime;
        $minutes = (int)$datetime->format('i');
        $datetime->setTime($datetime->format('H'), $minutes-$minutes%5, 0);
        return $datetime;
    }

    /**
     * @param $datetime \DateTime
     * @return \DateTime
     */
    static public function clampToMinute($datetime){
        $datetime = clone $datetime;
        $datetime->setTime($datetime->format('H'), $datetime->format('i'), 0);
        return $datetime;
    }

    /**
     * @param $start      DateTime
     * @param $end        DateTime
     * @param $resolution int
     *
     * Clamps the datetimes down to the nearest resolution step.
     * Also move the end datetime to the next step, so that
     * the entire step is included in the result.
     *
     * @throws InvalidArgumentException
     */
    static public function normalizeTimeRange(&$start, &$end, $resolution){
        if($resolution == Resolution::MINUTE){
            $start = DateTimeHelper::clampToMinute($start);
            $end = DateTimeHelper::clampToMinute($end);
            $end->add(DateInterval::createFromDateString('1 minute'));
        }else if($resolution == Resolution::FIVE_MINUTES){
            $start = DateTimeHelper::clampToFiveMin($start);
            $end = DateTimeHelper::clampToFiveMin($end);
            $end->add(DateInterval::createFromDateString('5 minutes'));
        }else if($resolution == Resolution::FIFTEEEN_MINUTES){
            $start = DateTimeHelper::clampToFifteenMin($start);
            $end = DateTimeHelper::clampToFifteenMin($end);
            $end->add(DateInterval::createFromDateString('15 minutes'));
        }else if($resolution == Resolution::HOUR){
            $start = DateTimeHelper::clampToHour($start);
            $end = DateTimeHelper::clampToHour($end);
            $end->add(DateInterval::createFromDateString('1 hour'));
        }else if($resolution == Resolution::DAY){
            $start = DateTimeHelper::clampToDay($start);
            $end = DateTimeHelper::clampToDay($end);
            $end->add(DateInterval::createFromDateString('1 day'));
        }else{
            throw new InvalidArgumentException('Invalid resolution given');
        }
    }
}