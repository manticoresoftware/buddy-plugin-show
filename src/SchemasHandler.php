<?php declare(strict_types=1);

/*
 Copyright (c) 2023, Manticore Software LTD (https://manticoresearch.com)

 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License version 2 or any later
 version. You should have received a copy of the GPL license along with this
 program; if you did not, you can find it at http://www.gnu.org/
 */

namespace Manticoresearch\Buddy\Plugin\Show;

use Manticoresearch\Buddy\Core\ManticoreSearch\Client as HTTPClient;
use Manticoresearch\Buddy\Core\Plugin\BaseHandlerWithTableFormatter;
use Manticoresearch\Buddy\Core\Plugin\TableFormatter;
use Manticoresearch\Buddy\Core\Task\Column;
use Manticoresearch\Buddy\Core\Task\Task;
use Manticoresearch\Buddy\Core\Task\TaskResult;
use RuntimeException;
use parallel\Runtime;

/**
 * This is the parent class to handle erroneous Manticore queries
 */
class SchemasHandler extends BaseHandlerWithTableFormatter {
	/**
	 *  Initialize the executor
	 *
	 * @param Payload $payload
	 * @return void
	 */
	public function __construct(public Payload $payload) {
	}

	/**
	 * Process the request and return self for chaining
	 *
	 * @return Task
	 * @throws RuntimeException
	 */
	public function run(Runtime $runtime): Task {
		$this->manticoreClient->setPath($this->payload->path);

		// We run in a thread anyway but in case if we need blocking
		// We just waiting for a thread to be done
		$taskFn = static function (string $args): TaskResult {
			/** @var Payload $payload */
			/** @var HTTPClient $manticoreClient */
			/** @var TableFormatter $tableFormatter */
			/** @phpstan-ignore-next-line */
			[$payload, $manticoreClient, $tableFormatter] = unserialize($args);
			$time0 = hrtime(true);
			// First, get response from the manticore
			$query = 'SHOW DATABASES';
			/** @var array{0:array{data:array<mixed>}} */
			$result = $manticoreClient->sendRequest($query)->getResult();
			$total = sizeof($result[0]['data']);
			if ($payload->hasCliEndpoint) {
				return TaskResult::raw($tableFormatter->getTable($time0, $result[0]['data'], $total));
			}
			return TaskResult::withData($result[0]['data'])
				->column('Database', Column::String);
		};

		return Task::createInRuntime(
			$runtime,
			$taskFn,
			[serialize([$this->payload, $this->manticoreClient, $this->tableFormatter])]
		)->run();
	}
}
