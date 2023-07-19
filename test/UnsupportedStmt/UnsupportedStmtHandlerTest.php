<?php declare(strict_types=1);

/*
  Copyright (c) 2023, Manticore Software LTD (https://manticoresearch.com)

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License version 2 or any later
  version. You should have received a copy of the GPL license along with this
  program; if you did not, you can find it at http://www.gnu.org/
*/

//use Manticoresearch\Buddy\Core\Error\QueryParseError;
use Manticoresearch\Buddy\Core\ManticoreSearch\Client as HTTPClient;
use Manticoresearch\Buddy\Core\ManticoreSearch\Endpoint as ManticoreEndpoint;
use Manticoresearch\Buddy\Core\ManticoreSearch\RequestFormat;
use Manticoresearch\Buddy\Core\ManticoreSearch\Response;
use Manticoresearch\Buddy\Core\Network\Request as NetRequest;
use Manticoresearch\Buddy\Core\Task\Task;
use Manticoresearch\Buddy\CoreTest\Trait\TestHTTPServerTrait;
use Manticoresearch\Buddy\CoreTest\Trait\TestInEnvironmentTrait;
use Manticoresearch\Buddy\Plugin\Show\Payload;
use Manticoresearch\Buddy\Plugin\Show\UnsupportedStmtHandler as Handler;
use PHPUnit\Framework\TestCase;

class UnsupportedStmtHandlerTest extends TestCase {

	use TestHTTPServerTrait;
	use TestInEnvironmentTrait;

	/**
	 * @var HTTPClient $manticoreClient
	 */
	public static $manticoreClient;

	public static function setUpBeforeClass(): void {
		self::setTaskRuntime();
		$serverUrl = self::setUpMockManticoreServer(false);
		self::setBuddyVersion();
		self::$manticoreClient = new HTTPClient(new Response(), $serverUrl);
		Payload::$type = 'unsupported';
	}

	public static function tearDownAfterClass(): void {
		self::finishMockManticoreServer();
	}

	public function testShowSessionStatusExecution():void {
		echo "\nTesting the execution of SHOW SESSION STATUS\n";
		$columns = [
			[
				'Variable_name' => ['type' => 'string'],
			],
			[
				'Value' => ['type' => 'string'],
			],
		];
		$data = [
			[],
		];
		$testingSet = [
			'SHOW SESSION STATUS',
			'show session status',
			'show session status from manticore',
		];

		foreach ($testingSet as $query) {
			$this->checkExecutionResult($query, $columns, $data);
		}
	}

	public function testShowTablesFromSchema():void {
		echo "\nTesting the execution of SHOW TABLES FROM information_schema\n";
		$columns = [
			[
				'Tables_in_information_schema' => ['type' => 'string'],
			],
		];
		$data = [
			[
				'Tables_in_information_schema' => 'COLUMNS',
			],
			[
				'Tables_in_information_schema' => 'COLUMN_STATISTICS',
			],
			[
				'Tables_in_information_schema' => 'FILES',
			],
			[
				'Tables_in_information_schema' => 'TABLES',
			],
			[
				'Tables_in_information_schema' => 'TRIGGERS',
			],
		];
		$testingSet = [
			'show tables from information_schema',
			'show tables from `information_schema`',
		];

		foreach ($testingSet as $query) {
			$this->checkExecutionResult($query, $columns, $data);
		}
	}

	public function testShowOpenTablesExecutionOk():void {
		echo "\nTesting the execution of SHOW OPEN TABLES FROM\n";
		$columns = [
			[
				'Index' => ['type' => 'string'],
			],
			[
				'Type' => ['type' => 'string'],
			],
		];
		$data = [
			[
				'Index' => 'test',
				'Type' => 'rt',
			],
		];
		$testingSet = [
			'SHOW OPEN TABLES FROM Manticore',
			'SHOW OPEN TABLES FROM `Manticore`',
			'show open tables from manticore',
		];

		foreach ($testingSet as $query) {
			$this->checkExecutionResult($query, $columns, $data);
		}
	}

	public function testShowOpenTablesExecutionFail():void {
		echo "\nTesting the execution of a SHOW OPEN TABLES syntax that is not handled by Buddy\n";
		$testingSet = [
			'SHOW OPEN TABLES FROM `Sometable`',
		];

		foreach ($testingSet as $query) {
			$this->checkExecutionResult($query, [], [], false);
		}
	}

	public function testShowTriggersExecution():void {
		echo "\nTesting the execution of SHOW TRIGGERS FROM\n";
		$columns = [
			[
				'Trigger' => ['type' => 'string'],
			],
			[
				'Event' => ['type' => 'string'],
			],
			[
				'Table' => ['type' => 'string'],
			],
			[
				'Statement' => ['type' => 'string'],
			],
			[
				'Timing' => ['type' => 'string'],
			],
			[
				'Created' => ['type' => 'string'],
			],
			[
				'sql_mode' => ['type' => 'string'],
			],
			[
				'Definer' => ['type' => 'string'],
			],
			[
				'character_set_client' => ['type' => 'string'],
			],
			[
				'collation_connection' => ['type' => 'string'],
			],
			[
				'Database Collation' => ['type' => 'string'],
			],
		];
		$data = [
			[],
		];
		$testingSet = [
			'SHOW TRIGGERS FROM Manticore',
			'SHOW TRIGGERS FROM `Manticore`',
		];

		foreach ($testingSet as $query) {
			$this->checkExecutionResult($query, $columns, $data);
		}
	}

	public function testShowEventsExecution():void {
		echo "\nTesting the execution of SHOW EVENTS FROM\n";
		$columns = [
			[
				'Db' => ['type' => 'string'],
			],
			[
				'Name' => ['type' => 'string'],
			],
			[
				'Definer' => ['type' => 'string'],
			],
			[
				'Time zone' => ['type' => 'string'],
			],
			[
				'Type' => ['type' => 'string'],
			],
			[
				'Execute at' => ['type' => 'string'],
			],
			[
				'Interval value' => ['type' => 'string'],
			],
			[
				'Interval field' => ['type' => 'string'],
			],
			[
				'Starts' => ['type' => 'string'],
			],
			[
				'Ends' => ['type' => 'string'],
			],
			[
				'Status' => ['type' => 'string'],
			],
			[
				'Originator' => ['type' => 'string'],
			],
			[
				'character_set_client' => ['type' => 'string'],
			],
			[
				'collation_connection' => ['type' => 'string'],
			],
			[
				'Database Collation' => ['type' => 'string'],
			],
		];
		$data = [
			[],
		];
		$testingSet = [
			'SHOW EVENTS FROM Manticore',
			'SHOW EVENTS FROM `Manticore`',
		];

		foreach ($testingSet as $query) {
			$this->checkExecutionResult($query, $columns, $data);
		}
	}

	public function testShowFunctionProcedureStatusExecution():void {
		echo "\nTesting the execution of SHOW FUNCTION/PROCEDURE STATUS\n";
		$columns = [
			[
				'Db' => ['type' => 'string'],
			],
			[
				'Name' => ['type' => 'string'],
			],
			[
				'Type' => ['type' => 'string'],
			],
			[
				'Definer' => ['type' => 'string'],
			],
			[
				'Modified' => ['type' => 'string'],
			],
			[
				'Created' => ['type' => 'string'],
			],
			[
				'Security_type' => ['type' => 'string'],
			],
			[
				'Comment' => ['type' => 'string'],
			],
			[
				'character_set_client' => ['type' => 'string'],
			],
			[
				'collation_connection' => ['type' => 'string'],
			],
			[
				'Database Collation' => ['type' => 'string'],
			],
		];
		$data = [
			[],
		];
		$testingSet = [
			'SHOW FUNCTION STATUS WHERE db="Manticore"',
			'SHOW PROCEDURE STATUS WHERE db="Manticore"',
		];

		foreach ($testingSet as $query) {
			$this->checkExecutionResult($query, $columns, $data);
		}
	}

	public function testShowCharacterSetExecution():void {
		echo "\nTesting the execution of SHOW CHARACTER SET\n";
		$columns = [
			[
				'Charset' => ['type' => 'string'],
			],
			[
				'Description' => ['type' => 'string'],
			],
			[
				'Default collation' => ['type' => 'string'],
			],
			[
				'Maxlen' => ['type' => 'long long'],
			],
		];
		$data = [
			[],
		];
		$testingSet = [
			'SHOW CHARACTER SET WHERE Charset="utf8mb4"',
			'SHOW CHARACTER SET LIKE "utf8%"',
		];

		foreach ($testingSet as $query) {
			$this->checkExecutionResult($query, $columns, $data);
		}
	}

	public function testShowEnginesExecution():void {
		echo "\nTesting the execution of SHOW ENGINES\n";
		$columns = [
			[
				'Engine' => ['type' => 'string'],
			],
			[
				'Support' => ['type' => 'string'],
			],
			[
				'Comment' => ['type' => 'string'],
			],
			[
				'Transactions' => ['type' => 'string'],
			],
			[
				'XA' => ['type' => 'string'],
			],
			[
				'Savepoints' => ['type' => 'string'],
			],
		];
		$data = [
			[
				'Engine' => 'MyISAM',
				'Support' => 'DEFAULT',
				'Comment' => 'MyISAM storage engine',
				'Transactions' => 'NO',
				'XA' => 'NO',
				'Savepoints' => 'NO',
			],
		];
		$testingSet = [
			'SHOW ENGINES',
		];

		foreach ($testingSet as $query) {
			$this->checkExecutionResult($query, $columns, $data);
		}
	}

	public function testShowCharsetExecution():void {
		echo "\nTesting the execution of SHOW CHARSET\n";
		$columns = [
			[
				'Charset' => ['type' => 'string'],
			],
			[
				'Description' => ['type' => 'string'],
			],
			[
				'Default collation' => ['type' => 'string'],
			],
			[
				'Maxlen' => ['type' => 'long long'],
			],
		];
		$data = [
			[
				'Charset' => 'utf8',
				'Description' => 'UTF-8 Unicode',
				'Default collation' => 'utf8_general_ci',
				'Maxlen' => 3,
			],
		];
		$testingSet = [
			'SHOW CHARSET',
		];

		foreach ($testingSet as $query) {
			$this->checkExecutionResult($query, $columns, $data);
		}
	}

	public function testShowVariablesExecutionOk():void {
		echo "\nTesting the execution of SHOW VARIABLES\n";
		$columns = [
			[
				'Variable_name' => ['type' => 'string'],
			],
			[
				'Value' => ['type' => 'string'],
			],
		];
		$data = [
			[
				'Variable_name' => 'autocommit',
				'Value' => 1,
			],
		];
		$testingSet = [
			'SHOW VARIABLES WHERE Variable_name="autocommit"',
			"SHOW VARIABLES WHERE `Variable_name`='autocommit'",
			"SHOW VARIABLES WHERE Variable_name IN ('autocommit', 'fake_var')",
			'SHOW VARIABLES WHERE Value="1"',
		];

		foreach ($testingSet as $query) {
			$this->checkExecutionResult($query, $columns, $data);
		}
	}

	public function testShowVariablesExecutionFail():void {
		echo "\nTesting the execution of a SHOW VARIABLES syntax that is not handled by Buddy\n";
		$testingSet = [
			'SHOW VARIABLES WHERE Value>="1"',
		];

		foreach ($testingSet as $query) {
			$this->checkExecutionResult($query, [], [], false);
		}
	}

	/**
	 * @param string $query
	 * @param array<mixed> $columns
	 * @param array<mixed> $data
	 * @param bool $isExecutionOk
	 * @return void
	*/
	protected function checkExecutionResult(
		string $query,
		array $columns,
		array $data,
		bool $isExecutionOk = true
	): void {
		$request = NetRequest::fromArray(
			[
				'error' => "P01: syntax error, unexpected identifier, expecting VARIABLES near 'STATUS'",
				'payload' => $query,
				'version' => 1,
				'format' => RequestFormat::SQL,
				'endpointBundle' => ManticoreEndpoint::Sql,
				'path' => 'sql?mode=raw',
			]
		);
		$payload = Payload::fromRequest($request);
		$handler = new Handler($payload);
		$handler->setManticoreClient(self::$manticoreClient);

		$task = $handler->run(Task::createRuntime());
		$task->wait();

		if ($isExecutionOk) {
			$this->assertEquals(true, $task->isSucceed());
			$result = $task->getResult()->getStruct();
			if (!isset($result[0]) || !is_array($result[0])) {
				$this->fail();
			}
			if (!isset($result[0]['columns'], $result[0]['data'])) {
				$this->fail();
			}
			$this->assertEquals($result[0]['columns'], $columns);
			$this->assertEquals($result[0]['data'], $data);
		} else {
			$this->assertEquals(false, $task->isSucceed());
			$this->assertEquals('RuntimeException: Cannot handle unsupported query', $task->getError()->getMessage());
		}
	}
}
