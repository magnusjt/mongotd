<?php namespace Mongotd\StorageMiddleware;

use Mongotd\Connection;
use Mongotd\CounterValue;
use Psr\Log\LoggerInterface;
use MongoDate;

class CalculateDeltas{
    /** @var  Connection */
    private $conn;

    /** @var  LoggerInterface */
    private $logger;

    /** @var int  */
    private $interval = 300;

    public function __construct(Connection $conn, LoggerInterface $logger, $interval = 300){
        $this->conn = $conn;
        $this->logger = $logger;
        $this->interval = $interval;
    }

    /**
     * @param $cvs CounterValue[]
     *
     * @return CounterValue[]
     */
    public function run(array $cvs){
        $cvsDelta = [];
        $col = $this->conn->col('cv_prev');

        foreach($cvs as $cv){
            if(!$cv->incremental){
                $cvsDelta[] = $cv;
                continue;
            }

            $timestamp = $cv->datetime->getTimestamp();
            $mongodate = new MongoDate($timestamp);

            $doc = $col->findOne(['sid' => $cv->sid, 'nid' => $cv->nid], ['mongodate' => 1, 'value' => 1]);
            if($doc){
                $delta = $this->getDeltaValue($cv->value, $doc['value'], $timestamp, $doc['mongodate']->sec);
                if($delta !== false){
                    $cv->value = $delta;
                    $cvsDelta[] = $cv;
                }
            }

            $col->update(['sid' => $cv->sid, 'nid' => $cv->nid],
                ['$set' => ['mongodate' => $mongodate, 'value' => $cv->value]],
                ['upsert' => true]);
        }

        return $cvsDelta;
    }

    /**
     * @param $value number
     * @param $valuePrev number
     * @param $timestamp int
     * @param $timestampPrev int
     * @return number|bool
     *
     * Returns difference between current and previous value.
     * Scales the result so it becomes the difference as it would
     * have been during a single interval. If the values are spaced
     * more than 3 intervals apart, false is returned to avoid
     * using delta-calculations between values too far apart.
     */
    private function getDeltaValue($value, $valuePrev, $timestamp, $timestampPrev){
        $secondsPast = $timestamp - $timestampPrev;

        if($value < $valuePrev){
            $this->logger->debug('Delta-calc failed because current value is less than previous value');
            return false;
        }

        if($secondsPast <= 0){
            $this->logger->debug('Delta-calc failed because number of seconds passed was <= 0');
            return false;
        }

        if($secondsPast > 6*$this->interval){
            $this->logger->debug('Delta-calc failed because more than 6 intervals passed before getting the next value');
            return false;
        }

        return ($value - $valuePrev) * ($this->interval / $secondsPast);
    }
}