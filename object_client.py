from minio import Minio

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
        return self.client.bucket_exists(bucket_name=bucket_name)
    
    # PUT object function
    def put_object(self, bucket_name, obj_name, obj_file):
        if not self.check_bucket(bucket_name):
            # self.client.make_bucket(bucket_name=bucket_name, location="us-east-1", object_lock=False)
            raise ValueError("Bucket does not exist")
        self.client.fput_object(bucket_name=bucket_name, object_name=obj_name, file_path=obj_file)
        
    # GET object function
    def get_object(self, bucket_name, obj_name, file_path):
        if not self.check_bucket(bucket_name):
            return None
    
        return self.client.fget_object(bucket_name=bucket_name, object_name=obj_name, file_path=file_path)            
        
    # DELETE object function
    def delete_object(self, bucket_name, obj_name):
        if not self.check_bucket(bucket_name):
            return None
        self.client.remove_object(bucket_name=bucket_name, object_name=obj_name)
        return 1
    
    # DELETE object list function
    def delete_objects(self, bucket_name, obj_name_list):
        if not self.check_bucket(bucket_name):
            return None
        self.client.remove_objects(bucket_name=bucket_name, delete_object_list=obj_name_list)
        return 1
    
    # # UPDATE object function
    # def update_object(self, bucket_name, obj_name):
    #     minio_client = self._client()
    #     if not self.check_bucket(bucket_name):
    
    # LIST object in a bucket
    def list_objects(self, bucket_name):
        if not self.check_bucket(bucket_name):
            return None
        return [obj.object_name for obj in self.client.list_objects(bucket_name=bucket_name, prefix=f"{bucket_name}")]
        
    