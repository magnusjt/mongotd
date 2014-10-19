<?php namespace Mongotd;

class Inserter{

    /** @var  GaugeInserter */
    private $gauge_inserter;

    /** @var  DeltaConverter */
    private $delta_converter;

    /** @var  AnomalyDetector */
    private $anomaly_detector;

    /** @var \DateTime */
    private $datetime = NULL;

    /** @var number[] */
    private $vals_by_sid = array();

    /** @var number[] */
    private $vals_by_sid_incremental = array();

    public function __construct($gauge_inserter, $delta_converter, $anomaly_detector){
        $this->gauge_inserter   = $gauge_inserter;
        $this->delta_converter  = $delta_converter;
        $this->anomaly_detector = $anomaly_detector;
        $this->utc_timezone     = new \DateTimeZone('UTC');
    }

    /**
     * @param $sid mixed
     * @param $value number
     * @param $is_incremental bool
     */
    public function add($sid, $value, $is_incremental = false){
        if(!is_numeric($value)){
            throw new \InvalidArgumentException('Value should be numeric');
        }

        if($is_incremental){
            $this->vals_by_sid_incremental[$sid] = $value;
        }else{
            $this->vals_by_sid[$sid] = $value;
        }
    }

    public function setDatetime(\DateTime $datetime){
        $this->datetime = clone $datetime;
        $this->datetime->setTimezone(new \DateTimeZone('UTC'));
    }

    public function execute(){
        if($this->datetime === NULL){
            throw new \InvalidArgumentException("DateTime wasn't set");
        }

        if(count($this->vals_by_sid_incremental) > 0){
            $vals_by_sid_delta_calculated = $this->delta_converter->convert($this->vals_by_sid_incremental, $this->datetime);

            foreach($vals_by_sid_delta_calculated as $sid => $val){
                $this->vals_by_sid[$sid] = $val;
            }
        }

        if(count($this->vals_by_sid) > 0){
            $this->gauge_inserter->addBatch($this->vals_by_sid, $this->datetime);
            $this->anomaly_detector->detectBatch($this->vals_by_sid, $this->datetime);
        }

        $this->vals_by_sid             = array();
        $this->vals_by_sid_incremental = array();
    }
}