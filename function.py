from googleapiclient.discovery import build


def trigger_df_job(cloud_event,environment):   
 
    service = build('dataflow', 'v1b3')
    project = "avd-databricks-demo-473209"

    template_path = "gs://dataflow-templates-us-central1/latest/GCS_Text_to_BigQuery"

    template_body = {
    "jobName": "bq-load",  # Provide a unique name for the job
    "parameters": {
        "javascriptTextTransformGcsPath": "gs://bkt-df-metadata-26/udf.js",
        "JSONPath": "gs://bkt-df-metadata-26/bq.json",
        "javascriptTextTransformFunctionName": "transform",
        "outputTable": "avd-databricks-demo-473209.user_data",
        "inputFilePattern": "gs://bkt-landing-zone/user.csv",
        "bigQueryLoadingTemporaryDirectory": "gs://bkt-df-metadata-26"
    }
    }

    request = service.projects().templates().launch(projectId=project,gcsPath=template_path, body=template_body)
    response = request.execute()
    print(response)

