<?php namespace Mongotd\Pipeline;

use DateTime;
use Exception;
use Mongotd\Aggregation;
use Mongotd\Connection;
use Mongotd\DateTimeHelper;
use Mongotd\Kpi\AstEvaluator;
use Mongotd\Kpi\Parser;
use Mongotd\Resolution;

class Factory{
    /** @var  Connection */
    private $conn;

    public function __construct(Connection $conn){
        $this->conn = $conn;
    }

    public function createFormulaAction($formula, $nid, DateTime $start, DateTime $end, $resolution, $padding){
        $parser = new Parser();
        $ast = $parser->parse($formula);

        $astEvaluator = new AstEvaluator();
        $astEvaluator->setPaddingValue($padding);
        $astEvaluator->setVariableEvaluatorCallback(function($options) use($nid, $start, $end, $resolution, $padding){
            if (!isset($options['sid'])) {
                throw new Exception('sid was not specified in variable. Need this to determine which sensor to get for the calculation of the formula.');
            }
            if (!isset($options['agg'])) {
                throw new Exception('agg was not specified in variable. Need this in order to aggregate up to the correct resolution before calculating formula.');
            }

            $pipeline = new Pipeline();
            return $pipeline->run([
                new Find($this->conn, $options['sid'], $nid, $start, $end),
                new RollupTime($resolution, $options['agg'], $padding),
                new Pad($resolution, $start, $end, $padding)
            ])->vals;
        });

        return new Formula($ast, $astEvaluator);
    }

    /**
     * @param string   $sid
     * @param string   $nid
     * @param DateTime $start
     * @param DateTime $end
     * @param int      $resultResolution
     * @param int      $resultAggregation
     * @param bool     $padding
     * @param int      $nodeResolution
     * @param int      $singleNodeAggregation
     * @param int      $combinedNodesAggregation
     * @param bool     $evaluateAsFormula
     * @param int      $formulaResolution
     *
     * @return array
     */
    public function createMultiAction(
        $sid, $nid, DateTime $start, DateTime $end,
        $resultResolution = Resolution::FIFTEEEN_MINUTES,
        $resultAggregation = Aggregation::SUM,
        $padding = false,
        $nodeResolution = Resolution::FIFTEEEN_MINUTES,
        $singleNodeAggregation = Aggregation::SUM,
        $combinedNodesAggregation = Aggregation::SUM,
        $evaluateAsFormula = false,
        $formulaResolution = Resolution::FIFTEEEN_MINUTES
    ){
        $start = clone $start;
        $end = clone $end;
        DateTimeHelper::normalizeTimeRange($start, $end, $resultResolution);

        if(is_array($nid) and count($nid) == 1){
            $nid = $nid[0];
        }

        $sequence = [];
        if(is_array($nid)){
            $nidSequence = [];
            foreach($nid as $aNid){
                $singleNidSequence = [];
                if($evaluateAsFormula){
                    $singleNidSequence[] = $this->createFormulaAction($sid, $aNid, $start, $end, $formulaResolution, $padding);
                }else{
                    $singleNidSequence[] = new Find($this->conn, $sid, $aNid, $start, $end);
                }

                $singleNidSequence[] = new RollupTime($nodeResolution, $singleNodeAggregation, $padding);
                $singleNidSequence[] = new Pad($nodeResolution, $start, $end, $padding);
                $nidSequence[] = $singleNidSequence;
            }

            $sequence[] = $nidSequence;
            $sequence[] = new RollupSpace($combinedNodesAggregation, $padding);
        }else{
            if($evaluateAsFormula){
                $sequence[] = $this->createFormulaAction($sid, $nid, $start, $end, $formulaResolution, $padding);
            }else{
                $sequence[] = new Find($this->conn, $sid, $nid, $start, $end);
            }
        }

        $sequence[] = new RollupTime($resultResolution, $resultAggregation, $padding);
        $sequence[] = new Pad($resultResolution, $start, $end, $padding);

        return $sequence;
    }

    /**
     * @param $start    DateTime
     * @param $end      DateTime
     * @param $nids     array
     * @param $sids     array
     * @param $minCount int
     * @param $limit    int
     *
     * @return array
     */
    public function createAnomalyAction(DateTime $start, DateTime $end, array $nids = array(), array $sids = array(), $minCount = 1, $limit = 20){
        return [
            new FindAnomalies($this->conn, $start, $end, $nids, $sids, $minCount, $limit)
        ];
    }
}