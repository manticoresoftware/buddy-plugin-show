<?php declare(strict_types=1);

/*
  Copyright (c) 2023, Manticore Software LTD (https://manticoresearch.com)

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License version 2 or any later
  version. You should have received a copy of the GPL license along with this
  program; if you did not, you can find it at http://www.gnu.org/
*/

namespace Manticoresearch\Buddy\Plugin\Show;

use Manticoresearch\Buddy\Core\ManticoreSearch\Client;
use Manticoresearch\Buddy\Core\Plugin\BaseHandlerWithClient;
use Manticoresearch\Buddy\Core\Task\Column;
use Manticoresearch\Buddy\Core\Task\Task;
use Manticoresearch\Buddy\Core\Task\TaskResult;
use RuntimeException;

final class VersionHandler extends BaseHandlerWithClient
{
	/**
	 * Initialize the executor
	 *
	 * @param  Payload  $payload
	 * @return void
	 */
	public function __construct(public Payload $payload) {
	}

	/**
	 * Process the request
	 *
	 * @return Task
	 * @throws RuntimeException
	 */
	public function run(): Task {
		$taskFn = static function (Client $manticoreClient): TaskResult {
			$query = "SHOW STATUS like 'version'";

			$result = $manticoreClient->sendRequest($query)->getResult();

			$versions = [];
			if (is_array($result) && isset($result[0]['data'][0]['Value'])) {
				$value = $result[0]['data'][0]['Value'];

				$splittedVersions = explode('(', $value);

				foreach ($splittedVersions as $version) {
					$version = trim($version);

					if ($version[mb_strlen($version) - 1] === ')') {
						$version = substr($version, 0, -1);
					}

					$exploded = explode(' ', $version);

					$component = 'Daemon';
					if (in_array($exploded[0], ['columnar', 'secondary', 'buddy'])) {
						$component = ucfirst($exploded[0]);
					} elseif ($exploded[0] === 'knn') {
						$component = 'KNN';
					}

					$versions[] = ['Component' => $component, 'Version' => $version];
				}
			}


			return TaskResult::withData($versions)
				->column('Component', Column::String)
				->column('Version', Column::String);
		};

		return Task::create(
			$taskFn, [$this->manticoreClient]
		)->run();
	}
}
