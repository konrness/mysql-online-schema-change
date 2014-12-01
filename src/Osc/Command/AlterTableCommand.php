<?php namespace Osc\Command;
use Psr\Log\AbstractLogger;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

class Log extends AbstractLogger
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

class AlterTableCommand extends Command
{
    const DEFAULT_USER = 'root';

    protected function configure()
    {
        $this
            ->setName('alter')
            ->setDescription('Runs an online alter table')
            ->addArgument('database', InputArgument::REQUIRED, 'The database')
            ->addArgument('table', InputArgument::REQUIRED, 'The table')
            ->addArgument('alter', InputArgument::REQUIRED, 'The alter statement')
            ->addOption('socket', null, InputOption::VALUE_REQUIRED, 'The socket to connect with')
            ->addOption('user', null, InputOption::VALUE_REQUIRED, 'The user to authenticate with', self::DEFAULT_USER)
            ->addOption('password', null, InputOption::VALUE_REQUIRED, 'The password to authenticate with')
        ;
    }
    /**
     * @param InputInterface $input
     * @param OutputInterface $output
     * @return int|null|void
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $verbosity = array(
            OutputInterface::VERBOSITY_NORMAL       => \Psr\Log\LogLevel::NOTICE,
            OutputInterface::VERBOSITY_VERY_VERBOSE => \Psr\Log\LogLevel::DEBUG,
            OutputInterface::VERBOSITY_VERBOSE      => \Psr\Log\LogLevel::INFO,
            OutputInterface::VERBOSITY_DEBUG        => \Psr\Log\LogLevel::DEBUG,
            OutputInterface::VERBOSITY_QUIET        => \Psr\Log\LogLevel::EMERGENCY,
        );

        $logger = new Log(STDOUT, $verbosity[$output->getVerbosity()]);

        if($socket = $input->getOption('socket'))
        {
            $socket = "unix_socket=$socket";
        }
        else
        {
            $socket = "host=localhost";
        }

        $pdo = new \PDO("mysql:$socket;", $input->getOption('user'), $input->getOption('password'), array(
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION
        ));

        $onlineSchemaChange = new \OnlineSchemaChangeRefactor(
            $pdo,
            $logger,
            $input->getArgument('database'),
            $input->getArgument('table'),
            $input->getArgument('alter'),
            null,
            OSC_FLAGS_ACCEPT_VERSION
        );

        try
        {
            $onlineSchemaChange->execute();
        }
        catch(\Exception $e)
        {
            $logger->error($e->getMessage());
        }

    }
}