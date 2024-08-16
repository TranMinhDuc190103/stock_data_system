from minio import Minio
from minio.commonconfig import CopySource

class MinioClient:
    
    def __init__(self, host, port, access_key, secret_key):
        self.host = host
        self.port = port
        self.access_key = access_key
        self.secret_key = secret_key
        self.client = Minio(
            endpoint=f"{self.host}:{self.port}",
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=False,
        )

    # MinIO client function
    def _client(self):
        return self.client
        
    # Check if bucket exists
    def check_bucket(self, bucket_name):
        if not self.client.bucket_exists(bucket_name=bucket_name):
            raise ValueError(f"Bucket {bucket_name} does not exist.")
    
    # Check if object in bucket exists
    def check_object(self, bucket_name, obj_name, prefix):
        self.check_bucket(bucket_name=bucket_name)
        list_objects = self.list_objects(bucket_name=bucket_name, prefix=prefix)
        if obj_name not in list_objects:
            raise ValueError(f"Object {obj_name} does not exist within bucket {bucket_name}.")
    
    # PUT object function
    def put_object(self, bucket_name, obj_name, obj_file, prefix=None):
        self.check_bucket(bucket_name)
        self.client.fput_object(bucket_name=bucket_name, object_name=obj_name, file_path=obj_file)
        
    # GET object function
    def get_object(self, bucket_name, obj_name, prefix=None):
        self.check_object(bucket_name=bucket_name, obj_name=obj_name, prefix=prefix)
        return self.client.get_object(bucket_name=bucket_name, object_name=obj_name)

    # LOAD object function
    def load_object(self, bucket_name, obj_name, obj_file, prefix=None):
        self.check_object(bucket_name=bucket_name, obj_name=obj_name, prefix=prefix)
        self.client.fget_object(bucket_name=bucket_name, object_name=obj_name, file_path=obj_file)
    
    # DELETE object function
    def delete_objects(self, bucket_name, obj_name_list, prefix=None):
        self.check_bucket(bucket_name)
        for obj_name in obj_name_list:
            self.check_object(bucket_name=bucket_name, obj_name=obj_name, prefix=prefix)
            self.client.remove_object(bucket_name=bucket_name, object_name=obj_name)
    
    # LIST object in a bucket
    def list_objects(self, bucket_name, prefix=None):
        self.check_bucket(bucket_name=bucket_name)
        return [obj.object_name for obj in self.client.list_objects(bucket_name=bucket_name, prefix=prefix)]

    # COPY object from one bucket to another
    def copy_object(self, bucket_name_from, bucket_name_to, obj_name, prefix_1=None, prefix_2=None):
        self.check_object(bucket_name=bucket_name_from, obj_name=obj_name, prefix=prefix_1)
        self.client.copy_object(bucket_name=bucket_name_to, object_name=f"{prefix_2}/{obj_name}", source=CopySource(bucket_name=bucket_name_from, object_name=obj_name))