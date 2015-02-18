<?php namespace Mongotd;

class Inserter{

    /** @var  GaugeInserter */
    private $gauge_inserter;

    /** @var  DeltaConverter */
    private $delta_converter;

    /** @var  AnomalyDetector */
    private $anomaly_detector;

    /** @var CounterValue[] */
    private $cvs = array();

    /** @var CounterValue[] */
    private $cvs_incremental = array();

    public function __construct($gauge_inserter, $delta_converter, $anomaly_detector){
        $this->gauge_inserter   = $gauge_inserter;
        $this->delta_converter  = $delta_converter;
        $this->anomaly_detector = $anomaly_detector;

        $this->counter_values             = array();
        $this->counter_values_incremental = array();
    }

    /**
     * @param $sid            int|string
     * @param $nid            int|string
     * @param $datetime       \DateTime
     * @param $value          number
     * @param $is_incremental bool
     */
    public function add($sid, $nid, $datetime, $value, $is_incremental = false){
        if(!is_numeric($value)){
            throw new \InvalidArgumentException('Value should be numeric');
        }

        $datetime = clone $datetime;
        $datetime->setTimezone(new \DateTimeZone('UTC'));
        $cv = new CounterValue($sid, $nid, $datetime, $value);

        if($is_incremental){
            $this->cvs_incremental[] = $cv;
        }else{
            $this->cvs[] = $cv;
        }
    }

    public function insert(){
        if(count($this->cvs_incremental) > 0){
            $cvs = $this->delta_converter->convert($this->cvs_incremental);
            $this->cvs = array_merge($this->cvs, $cvs);
        }

        if(count($this->cvs) > 0){
            $this->gauge_inserter->addBatch($this->cvs);
            $this->anomaly_detector->detectBatch($this->cvs);
        }

        $this->cvs = array();
        $this->cvs_incremental = array();
    }
}