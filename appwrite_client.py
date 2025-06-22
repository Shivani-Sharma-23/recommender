import os
from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.query import Query
from appwrite.exception import AppwriteException
from dotenv import load_dotenv
import json

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
        self.user_activity_collection_id = os.getenv('USER_ACTIVITY_COLLECTION_ID', 'user_activity')
    
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
    
    def get_job(self, job_id):
        """Get a specific job by job ID"""
        try:
            return self.databases.get_document(
                database_id=self.database_id,
                collection_id=self.jobs_collection_id,
                document_id=job_id
            )
        except Exception as e:
            print(f"Error fetching job {job_id}: {e}")
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
    
    def get_user_activity(self, user_id):
        """Get user activity record"""
        try:
            return self.databases.get_document(
                database_id=self.database_id,
                collection_id=self.user_activity_collection_id,
                document_id=user_id
            )
        except AppwriteException as e:
            if e.code == 404:  # Document not found
                return None
            print(f"Error fetching user activity: {e}")
            return None
        except Exception as e:
            print(f"Error fetching user activity: {e}")
            return None
    
    def update_user_activity(self, user_id, job_id):
        """Update user activity with new job click, maintaining only last 10 activities"""
        try:
            # Get existing activity
            existing_activity = self.get_user_activity(user_id)
            
            # Prepare recent activities list
            recent_activities = []
            
            if existing_activity:
                # Extract existing activities
                for i in range(1, 11):  # recent_activity to recent_activity_10
                    activity_key = f'recent_activity_{i}' if i > 1 else 'recent_activity'
                    activity_value = existing_activity.get(activity_key, '0')
                    if activity_value and activity_value != '0':
                        recent_activities.append(activity_value)
            
            # Add new job_id at the beginning
            recent_activities.insert(0, job_id)
            
            # Keep only last 10 activities
            recent_activities = recent_activities[:10]
            
            # Prepare update data
            update_data = {'userId': user_id}
            
            # Fill the recent_activity fields
            for i in range(10):
                activity_key = f'recent_activity_{i+1}' if i > 0 else 'recent_activity'
                if i < len(recent_activities):
                    update_data[activity_key] = recent_activities[i]
                else:
                    update_data[activity_key] = '0'
            
            if existing_activity:
                # Update existing document
                return self.databases.update_document(
                    database_id=self.database_id,
                    collection_id=self.user_activity_collection_id,
                    document_id=user_id,
                    data=update_data
                )
            else:
                # Create new document
                return self.databases.create_document(
                    database_id=self.database_id,
                    collection_id=self.user_activity_collection_id,
                    document_id=user_id,
                    data=update_data
                )
                
        except Exception as e:
            print(f"Error updating user activity: {e}")
            return None
    
    def get_user_recent_activities_with_jobs(self, user_id):
        """Get user's recent activities with full job data"""
        try:
            activity_data = self.get_user_activity(user_id)
            if not activity_data:
                return []
            
            recent_jobs = []
            
            # Get job data for each recent activity
            for i in range(1, 11):  # recent_activity to recent_activity_10
                activity_key = f'recent_activity_{i}' if i > 1 else 'recent_activity'
                job_id = activity_data.get(activity_key, '0')
                
                if job_id and job_id != '0':
                    job_data = self.get_job(job_id)
                    if job_data:
                        recent_jobs.append(job_data)
            
            return recent_jobs
            
        except Exception as e:
            print(f"Error fetching user recent activities with jobs: {e}")
            return []
    
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