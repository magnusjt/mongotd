<?php
namespace Mongotd;

/*
 * Class takes a list of nids/sids to scan for anomalies using 3 sigma method.
 * It looks at values at the same time period each day for a specified number of days back in time.
 * If the current value differs more than 3 standard deviations from the average at this time of day,
 * the value is considered an anomaly.
 *
 * There is a smoothing interval that can be set so that instead of looking at
 * a given point in time, we look at the average during the interval.
 */
class AnomalyScanner3Sigma extends AnomalyScanner{
    /** @var int  */
    private $daysToScan = 20;

    /** @var int  */
    private $windowLengthInSeconds = 300;

    /** @var int How many std's of deviation we accept (default to 3, as in 3-sigma) */
    private $scoreTreshold = 3;

    /**
     * @param $cvs      CounterValue[]
     * @param $datetime \DateTime
     *
     * @return array
     */
    public function scan($cvs, \DateTime $datetime){
        $datetimeToCheck = clone $datetime;
        $datetimeToCheck->setTimeZone(new \DateTimeZone('UTC'));
        $windowEndPosition = $datetimeToCheck->getTimestamp()%86400;

        $datetimeToCheck->setTime(0, 0, 0);
        $controlStartDate = clone $datetimeToCheck;
        $controlEndDate = clone $datetimeToCheck;
        $controlStartDate->sub(\DateInterval::createFromDateString($this->daysToScan . ' days'));
        $controlEndDate->sub(\DateInterval::createFromDateString('1 day'));

        $controlMongodateStart = new \MongoDate($controlStartDate->getTimestamp());
        $controlMongodateEnd = new \MongoDate($controlEndDate->getTimestamp());
        $mongodateToCheck = new \MongoDate($datetimeToCheck->getTimestamp());

        foreach($cvs as $cv){
            $prevDataPoints = $this->getValsWithinWindow($cv->nid, $cv->sid, $controlMongodateStart, $controlMongodateEnd, $this->windowLengthInSeconds, $windowEndPosition);
            $currDataPoints = $this->getValsWithinWindow($cv->nid, $cv->sid, $mongodateToCheck, $mongodateToCheck, $this->windowLengthInSeconds, $windowEndPosition);
            $predicted = $this->checkForAnomaly($prevDataPoints, $currDataPoints);
            if($predicted !== false){
                $this->storeAnomaly($cv->nid, $cv->sid, $datetime, $predicted, $cv->value);
            }
        }
    }

    /**
     * @param $prevDataPoints number[]
     * @param $currDataPoints number[]
     *
     * @return bool|number
     */
    private function checkForAnomaly(array $prevDataPoints, array $currDataPoints){
        if(count($prevDataPoints) == 0 or count($currDataPoints) == 0){
            return false;
        }

        $prevStats = $this->calcAvgAndStd($prevDataPoints);
        $currStats = $this->calcAvgAndStd($currDataPoints);

        if($prevStats['std'] == 0){
            return false;
        }

        $score = abs($currStats['avg'] - $prevStats['avg'])/$prevStats['std'];
        if($score > $this->scoreTreshold){
            return $prevStats['avg'];
        }

        return false;
    }

    /**
     * @param $vals number[]
     *
     * @return array
     */
    private function calcAvgAndStd(array $vals){
        $sum = array_sum($vals);
        $n = count($vals);
        $avg = $sum/$n;
        $var = 0;

        foreach($vals as $val){
            $var += pow($val - $avg, 2);
        }

        $var /= $n;
        $std = sqrt($var);

        return array('avg' => $avg, 'std' => $std);
    }
}