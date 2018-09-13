from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

from datetime import datetime

def create_dag(dag_id, value):
	def run_print_var():
		if value == 4:
			return "kwas_print_var"
		elif value == 3:
			return "go_fail"
		else:
			return "print_var"

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

	print_var = BashOperator(
		task_id='print_var',
		bash_command='echo "var: {{ dag_run.conf["var"] }}"',
		dag=dag
	)
	print_var.set_upstream(branch)

	kwas_print_var = BashOperator(
		task_id='kwas_print_var',
		bash_command='echo "KWAS: {{ dag_run.conf["var"] }}"',
		dag=dag
	)
	kwas_print_var.set_upstream(branch)

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
	finish.set_upstream(print_var)
	finish.set_upstream(kwas_print_var)
	finish.set_upstream(fail)

	return dag

for n in range(1,5):
	dag_id="kwas_%s" % (n)
	globals()[dag_id] = create_dag(dag_id, n)
