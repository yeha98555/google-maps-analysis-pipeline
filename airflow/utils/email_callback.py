from airflow.models import Variable
from airflow.utils.email import send_email


def failure_callback(context: dict) -> None:
    """
    Send email to alert email when the task is failed

    Args:
        context (dict): The context dictionary

    Returns:
        None
    """
    subject = f"Airflow Task Failure: {context['task_instance_key_str']}"
    html_content = f"""
    <h3>Task Failed</h3>
    <p>Task: {context['task_instance'].task_id}</p>
    <p>DAG: {context['task_instance'].dag_id}</p>
    <p>Execution Time: {context['execution_date']}</p>
    <p>Log URL: <a href="{context['task_instance'].log_url}">Click here</a></p>
    <p>Error: {context['exception']}</p>
    """
    send_email(
        to=Variable.get("alert_email", deserialize_json=True),
        subject=subject,
        html_content=html_content,
    )


def success_callback(context: dict) -> None:
    """
    Send email to alert email when the task is succeeded

    Args:
        context (dict): The context dictionary

    Returns:
        None
    """
    subject = f"Airflow Task Success: {context['task_instance_key_str']}"
    html_content = f"""
    <h3>Task Succeeded</h3>
    <p>Task: {context['task_instance'].task_id}</p>
    <p>DAG: {context['task_instance'].dag_id}</p>
    <p>Execution Time: {context['execution_date']}</p>
    <p>Log URL: <a href="{context['task_instance'].log_url}">Click here</a></p>
    """
    send_email(
        to=Variable.get("alert_email", deserialize_json=True),
        subject=subject,
        html_content=html_content,
    )


def info_gsheet_callback(context: dict, url: str) -> None:
    """
    Notify the PM the Google Sheet is created, please check the attraction list

    Args:
        context (dict): The context dictionary

    Returns:
        None
    """
    subject = "Attraction List is created"
    html_content = f"""
    <h3>Attraction List is created</h3>
    <p>Google Sheet URL: <a href="{url}">Click here</a></p>
    <p>Please check the attraction list. When you are done, please send a message.</p>
    ---
    <p>Task: {context['task_instance'].task_id}</p>
    <p>DAG: {context['task_instance'].dag_id}</p>
    """
    send_email(
        to=Variable.get("pm_email", deserialize_json=True),
        subject=subject,
        html_content=html_content,
        cc=Variable.get("alert_email", deserialize_json=True),
    )
