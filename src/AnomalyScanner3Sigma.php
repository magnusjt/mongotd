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
     * @param $cvs CounterValue[]
     *
     * @return array
     */
    public function scan($cvs){
        foreach($cvs as $cv){
            $datetimeMinusOneDay = clone $cv->datetime;
            $datetimeMinusOneDay->sub(\DateInterval::createFromDateString('1 day'));

            $prevDataPoints = $this->getValsWithinWindows($cv->nid, $cv->sid, $datetimeMinusOneDay, $this->daysToScan, $this->windowLengthInSeconds);

            // Prevent anomaly detection when there is little to no data
            if(count($prevDataPoints) < $this->daysToScan){
                continue;
            }

            $currDataPoints = $this->getValsWithinWindows($cv->nid, $cv->sid, $cv->datetime, 0, $this->windowLengthInSeconds);
            $predicted = $this->checkForAnomaly($prevDataPoints, $currDataPoints);
            if($predicted !== false){
                $this->storeAnomaly($cv->nid, $cv->sid, $cv->datetime, $predicted, $cv->value);
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