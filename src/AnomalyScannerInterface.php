<?php namespace Mongotd;

interface AnomalyScannerInterface{
    public function scan(array $cvs);
}