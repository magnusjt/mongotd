<?php namespace Mongotd;

use \Psr\Log\LoggerInterface;

class DeltaConverter{
    /** @var  Connection */
    private $conn;

    /** @var  LoggerInterface */
    private $logger;

    /** @var int  */
    private $resolution;

    public function __construct($conn, $resolution, LoggerInterface $logger = NULL){
        $this->conn   = $conn;
        $this->logger = $logger;
        $this->resolution = $resolution;

        if($this->resolution > Resolution::HOUR){
            $this->resolution = Resolution::HOUR;
        }
    }

    /**
     * @param $cvs CounterValue[]
     *
     * @return array
     */
    public function convert($cvs){
        $col = $this->conn->col('cv_prev');
        $batch_updater  = new \MongoUpdateBatch($col);
        $cvs_delta = array();

        foreach($cvs as $cv){
            $mongodate = new \MongoDate($cv->datetime->getTimestamp());
            $batch_updater->add(array(
                                    'q' => array('sid' => $cv->sid, 'nid' => $cv->nid),
                                    'u' => array('$set' => array('mongodate' => $mongodate, 'value' => $cv->value)),
                                    'upsert' => true
                                ));

            $doc = $col->findOne(array('sid' => $cv->sid, 'nid' => $cv->nid), array('mongodate' => 1, 'value' => 1));
            if($doc){
                $datetime_prev = new \DateTime();
                $datetime_prev->setTimezone(new \DateTimeZone('UTC'));
                $datetime_prev->setTimestamp($doc['mongodate']->sec);
                $delta = $this->getDeltaValue($cv->value, $doc['value'], $cv->datetime, $datetime_prev);
                if($delta !== false){
                    $cvs_delta[] = new CounterValue($cv->sid, $cv->nid, $cv->datetime, $delta);
                }
            }
        }

        $batch_updater->execute(array('w' => 1));

        return $cvs_delta;
    }

    private function getDeltaValue($value, $value_prev, $datetime, $datetime_prev){
        $minutes_past = $datetime->diff($datetime_prev)->i;

        $delta_value = false;
        if($minutes_past > 0 and $minutes_past < 60){
            $delta_value = ($value - $value_prev) * ($this->resolution / $minutes_past);
            if($delta_value < 0){
                $delta_value = false;
            }
        }

        return $delta_value;
    }
}