from tempfile import NamedTemporaryFile

from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook


class HttpToGcsOperator(BaseOperator):
    """
    Calls an endpoint on an HTTP system to execute an action
    :param http_conn_id: The connection to run the operator against
    :type http_conn_id: string
    :param endpoint: The relative part of the full url. (templated)
    :type endpoint: string
    :param gcs_path: The path of the GCS to store the result
    :type gcs_path: string
    """

    template_fields = ("http_conn_id", "endpoint", "gcs_path", "gcs_bucket", "method")
    template_ext = ()
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(
            self,
            endpoint,
            gcs_bucket,
            gcs_path,
            method="GET",
            gcs_conn_id="gcs_default",
            http_conn_id="http_default",
            *args, **kwargs):
        super(HttpToGcsOperator, self).__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.endpoint = endpoint
        self.gcs_path = gcs_path
        self.gcs_conn_id = gcs_conn_id
        self.method = method
        self.gcs_bucket = gcs_bucket

    def execute(self, context):
        http = HttpHook(method=self.method, http_conn_id=self.http_conn_id)

        data = http.run(endpoint=self.endpoint)

        with NamedTemporaryFile() as tmp_file_handle:
            tmp_file_handle.write(data.content)
            tmp_file_handle.flush()

            gcs = GoogleCloudStorageHook(google_cloud_storage_conn_id=self.gcs_conn_id)
            gcs.upload(bucket=self.gcs_bucket, object=self.gcs_path, filename=tmp_file_handle.name)
