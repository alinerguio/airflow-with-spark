import os 
import datetime

from pandas import DataFrame

from airflow.decorators import dag
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.slack.notifications.slack import send_slack_notification

from airflow.models import Variable

env = "development" # variables
domain = "assessments" # variables
table_name = "reviews" 
zone = "raw" # variables

dag_prefix = "source_to_raw" if zone == "raw" else "raw_to_trusted" if zone == "trusted" else "trusted_to_refined" if zone == "refined" else "refined_to_analytics" 
dag_name = f"{env}_{dag_prefix}_{domain}_{table_name}"

channel="#b-data-pipelines-alerts" # variables

succ_env_message = "[✅🏭 production] " if env == "production" else "[🔜⚡️ staging] " if env == "staging" else  "[👩‍💻 development] " 
fail_env_message = "[❌❌🏭 production] " if env == "production" else "[❗🔜⚡️ staging] " if env == "staging" else  "[❗👩‍💻 development] "

dag_failure_slack_notification = send_slack_notification(
    text= fail_env_message + "The dag **{{ dag.dag_id }}** failed", 
    channel=channel,
)
## look into markdown to improve messages
dag_success_slack_notification = send_slack_notification(
    text= succ_env_message + "The dag **{{ dag.dag_id }}** ran successfully, data should be in Delta Lake table in " + f"https://s3.console.aws.amazon.com/s3/buckets/b-data-{env}?region=us-east-1&prefix={zone}/{domain}/{table_name}/&showversions=false", 
    channel=channel,
)

task_failure_slack_notification = send_slack_notification( # improve
    text=fail_env_message + "The task **{{ ti.task_id }}** failed",
    channel=channel,
)

task_quality_failure_slack_notification = send_slack_notification( # improve
    text=fail_env_message + "The quality task **{{ ti.task_id }}** failed, review data",
    channel=channel,
)

with DAG(
    dag_id=dag_name,
    start_date=datetime.datetime(2024, 1, 9),
    schedule="@daily",
    # on_failure_callback=[dag_failure_slack_notification],
    # on_success_callback=[dag_success_slack_notification],
    tags=[env, domain, table_name, zone],
    dagrun_timeout=datetime.timedelta(minutes=20),
) as dag:
    @task(task_id="transform_data")#, on_failure_callback=[task_failure_slack_notification], execution_timeout=datetime.timedelta(seconds=60))
    def transform_data(data: list, columns: list) -> DataFrame: # create operator
        import pandas as pd
        try:
            columns_df = [c[0] for c in columns]
            df = pd.DataFrame(data=data, columns=columns_df)

            for col in df.columns: # Invalid data type for Delta Lake: Null
                col_t = df[col].dtype
                if col_t=='object':
                    df[col] = df[col].fillna("")
            
            df['inserted_at'] = datetime.datetime.now() # inserted at data platform
                    
            return df
        except Exception as error:
            print(error)


    @task(task_id="test_data")#, on_failure_callback=[task_quality_failure_slack_notification], execution_timeout=datetime.timedelta(seconds=60))
    def test_data(df: DataFrame): # create operator with tests as param 
        import great_expectations as ge
        ge_df = ge.from_pandas(
            df
        )
        tests = [ge_df.expect_column_values_to_be_between("cost_plus_points", 0, 4)]#, 
                    # ge_df.expect_column_values_to_not_be_null("survey_sent_at")]
        results = [t["success"] for t in tests]
        if False in results:
            raise AirflowException("Quality check failed, review data")
    

    @task(task_id="write_data")#, on_failure_callback=[task_failure_slack_notification], execution_timeout=datetime.timedelta(seconds=60))
    def write_data(df: DataFrame): # create operator
        from deltalake import write_deltalake
        try:
            ACCESS_KEY=''
            SECRET_KEY=''
            # TREAT EXCEPTION IF ACCESS_KEY AND SECRET_KEY DON'T EXIST, AREN'T STRINGS, != ''

            storage_options = {"AWS_ACCESS_KEY_ID": ACCESS_KEY, "AWS_SECRET_ACCESS_KEY": SECRET_KEY, "AWS_REGION": "us-east-1", "AWS_S3_ALLOW_UNSAFE_RENAME": "true"}
            write_deltalake(f"s3://b-data-{env}/{zone}/{domain}/{table_name}/", df, storage_options=storage_options, mode='append')
        except Exception as error:
            print(error)

    get_data = PostgresOperator(
        task_id="get_data",
        postgres_conn_id="postgres_assessments",
        sql=f"SELECT * FROM {table_name};",
        # on_failure_callback=[task_failure_slack_notification],
        execution_timeout=datetime.timedelta(seconds=240)
    )

    get_columns = PostgresOperator(
        task_id="get_columns",
        postgres_conn_id="postgres_assessments",
        sql=f"SELECT column_name FROM information_schema.columns WHERE table_name='{table_name}';",
        # on_failure_callback=[task_failure_slack_notification],
        execution_timeout=datetime.timedelta(seconds=60)
    )

    transform_data = transform_data(get_data.output, get_columns.output)
    test_data = test_data(transform_data)
    write_data = write_data(transform_data)

    QUERY_CREATE_TABLE = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS `b_data_{env}_{zone}`.`{domain}_{table_name}`
    LOCATION 's3://b-data-{env}/{zone}/{domain}/{table_name}/'
    TBLPROPERTIES ('table_type' = 'DELTA');
    """

    create_table_athena = AthenaOperator(
        task_id="create_table_athena",
        query=QUERY_CREATE_TABLE,
        database=f'b_data_{env}',
        output_location=f's3://b-data-{env}-queries/',
        sleep_time=30,
        # on_failure_callback=[task_failure_slack_notification],
        execution_timeout=datetime.timedelta(seconds=60)
    )

    [get_data, get_columns] >> transform_data >> test_data >> write_data >> create_table_athena