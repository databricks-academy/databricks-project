clean-pyc:
		find . | grep -E "(__pycache__|\.pyc|\.pyo)" | xargs rm -rf

create-cluster:
		databricks clusters create --json-file env/cluster.json

generate-test-data:
		docker-compose exec spark_testing python scripts/generate_test_data.py

launch-test-server:
		docker-compose up -d

run-test: launch-test-server clean-pyc
		docker-compose exec spark_testing pytest -s $(testfile)

run-tests: launch-test-server clean-pyc
		docker-compose exec spark_testing pytest -s

wheel:
		python pipelines/setup.py bdist_wheel

wheel-install: wheel
	  databricks libraries install --cluster-id $(cluster_id) --whl file:$(bdist_wheel)
