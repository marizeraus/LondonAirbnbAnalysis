setup-application:
	python3 -m venv venv && . ./venv/bin/activate && pip install -r requirements.txt

start:
	/opt/hadoop/sbin/start-dfs.sh
	/opt/hadoop/sbin/start-yarn.sh
	docker start hivemetastore
	docker ps

run:
	python3 spark/run.py

stop:
	/opt/hadoop/sbin/stop-yarn.sh
	/opt/hadoop/sbin/stop-dfs.sh