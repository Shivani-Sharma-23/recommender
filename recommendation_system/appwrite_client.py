import os
from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.query import Query
from dotenv import load_dotenv

load_dotenv()

class AppwriteClient:
    def __init__(self):
        self.client = Client()
        self.client.set_endpoint(os.getenv('APPWRITE_ENDPOINT'))
        self.client.set_project(os.getenv('APPWRITE_PROJECT_ID'))
        self.client.set_key(os.getenv('APPWRITE_API_KEY'))
        
        self.databases = Databases(self.client)
        self.database_id = os.getenv('APPWRITE_DATABASE_ID', 'gigrithm')
        self.jobs_collection_id = os.getenv('JOBS_COLLECTION_ID')
        self.users_collection_id = os.getenv('USERS_COLLECTION_ID')
    
    def get_user(self, user_id):
        try:
            return self.databases.get_document(
                database_id=self.database_id,
                collection_id=self.users_collection_id,
                document_id=user_id
            )
        except Exception as e:
            print(f"Error fetching user: {e}")
            return None
    
    def get_jobs(self, limit=100):
        try:
            return self.databases.list_documents(
                database_id=self.database_id,
                collection_id=self.jobs_collection_id,
                queries=[Query.limit(limit)]
            )
        except Exception as e:
            print(f"Error fetching jobs: {e}")
            return None
    
    def get_jobs_by_filters(self, location=None, job_type=None, experience_level=None):
        queries = [Query.limit(500)]  # Add default limit
        
        if location and location != 'all':
            queries.append(Query.equal('location', location))
        if job_type and job_type != 'all':
            queries.append(Query.equal('jobType', job_type))
        if experience_level and experience_level != 'all':
            queries.append(Query.equal('experienceLevel', experience_level))
        
        try:
            return self.databases.list_documents(
                database_id=self.database_id,
                collection_id=self.jobs_collection_id,
                queries=queries
            )
        except Exception as e:
            print(f"Error fetching filtered jobs: {e}")
            return None
    
    def test_connection(self):
        """Test the Appwrite connection"""
        try:
            # Try to list documents with a small limit to test connection
            result = self.databases.list_documents(
                database_id=self.database_id,
                collection_id=self.jobs_collection_id,
                queries=[Query.limit(1)]
            )
            return True, "Connection successful"
        except Exception as e:
            return False, f"Connection failed: {str(e)}"