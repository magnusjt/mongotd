<?php namespace Mongotd;

use DateTime;
use Exception;
use Mongotd\Kpi\AstEvaluator;
use Mongotd\Kpi\Parser;
use Mongotd\Pipeline\Find;
use Mongotd\Pipeline\FindAnomalies;
use Mongotd\Pipeline\Formula;
use Mongotd\Pipeline\Pad;
use Mongotd\Pipeline\Pipeline;
use Mongotd\Pipeline\RollupSpace;
use Mongotd\Pipeline\RollupTime;

class Retriever{
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
            ]);
        });

        return new Formula($ast, $astEvaluator);
    }

    public function get(
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

        $pipeline = new Pipeline();
        $sequence = [];

        if(is_array($nid)){
            foreach($nid as $aNid){
                $subSequence = [];
                if($evaluateAsFormula){
                    $subSequence[] = $this->createFormulaAction($sid, $aNid, $start, $end, $formulaResolution, $padding);
                }else{
                    $subSequence[] = new Find($this->conn, $sid, $aNid, $start, $end);
                }

                $subSequence[] = new RollupTime($nodeResolution, $singleNodeAggregation, $padding);
                $subSequence[] = new Pad($nodeResolution, $start, $end, $padding);
                $sequence[] = $subSequence;
            }

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

        return $pipeline->run($sequence)->vals;
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
    public function getAnomalies(DateTime $start, DateTime $end, array $nids = array(), array $sids = array(), $minCount = 1, $limit = 20){
        $pipeline = new Pipeline();

        return $pipeline->run([
            new FindAnomalies($this->conn, $start, $end, $nids, $sids, $minCount, $limit)
        ]);
    }
}