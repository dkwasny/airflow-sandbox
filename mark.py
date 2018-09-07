from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

from datetime import datetime

def create_dag(dag_id, value):
	def run_print_var():
		return "go_fail"

	default_args = {
		'owner': 'kwas',
		'start_date': datetime(2018, 9, 6),
		'var': 'default'
	}

	dag = DAG(dag_id, default_args=default_args)

	print_date = BashOperator(
		task_id='print_date',
		bash_command='date',
		dag=dag
	)

	branch = BranchPythonOperator(
		task_id='branch',
		python_callable=run_print_var,
		dag=dag
	)
	branch.set_upstream(print_date)

	fail = BashOperator(
		task_id='go_fail',
		bash_command='if [ ! -f /tmp/kwas-fail ]; then exit 1; fi',
		dag=dag
	)
	fail.set_upstream(branch)

	finish = BashOperator(
		task_id='final_task',
		bash_command='echo finish',
		trigger_rule='all_success',
		dag=dag
	)
	finish.set_upstream(fail)

	return dag

for n in range(1,2):
	dag_id="mark_%s" % (n)
	globals()[dag_id] = create_dag(dag_id, n)
