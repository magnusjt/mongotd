<?php
namespace Mongotd;

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
        $datetime = new \DateTime($datetime->format('o-\WW-1')); // 1 is monday. o is year taking into account week number 53 and such.
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

}