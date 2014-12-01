<?php namespace Osc;

use Psr\Log\AbstractLogger;

class Logger extends AbstractLogger
{
    protected $levelMapping = array(
        \Psr\Log\LogLevel::DEBUG        => LOG_DEBUG,
        \Psr\Log\LogLevel::INFO         => LOG_INFO,
        \Psr\Log\LogLevel::NOTICE       => LOG_NOTICE,
        \Psr\Log\LogLevel::WARNING      => LOG_WARNING,
        \Psr\Log\LogLevel::ERROR        => LOG_ERR,
        \Psr\Log\LogLevel::CRITICAL     => LOG_CRIT,
        \Psr\Log\LogLevel::ALERT        => LOG_ALERT,
        \Psr\Log\LogLevel::EMERGENCY    => LOG_EMERG,
    );

    protected $file;

    protected $verbosity;

    public function __construct($fileHandle, $verbosity = 0)
    {
        $this->file = is_array($fileHandle) ? $fileHandle : array($fileHandle);

        $this->verbosity = $this->levelMapping[$verbosity];
    }

    public function log($level, $message, array $context = array())
    {
        if($this->levelMapping[$level] <= $this->verbosity)
        {
            foreach($this->file as $handle)
            {
                fwrite($handle, "$level: $message\n");
            }
        }
    }
}