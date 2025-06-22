# FIXED: appwrite_client.py - Enhanced error handling and logging

import os
from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.query import Query
from appwrite.exception import AppwriteException
from dotenv import load_dotenv
import json
import logging

load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
        
        # FIXED: Make sure collection ID matches your actual collection
        self.user_activity_collection_id = os.getenv('USER_ACTIVITY_COLLECTION_ID', 'user_activity')
        
        # Log configuration for debugging
        logger.info(f"Appwrite Client initialized:")
        logger.info(f"  Database ID: {self.database_id}")
        logger.info(f"  User Activity Collection ID: {self.user_activity_collection_id}")
    
    def get_user_activity(self, user_id):
        """Get user activity record with enhanced error handling"""
        try:
            logger.info(f"Fetching user activity for user: {user_id}")
            result = self.databases.get_document(
                database_id=self.database_id,
                collection_id=self.user_activity_collection_id,
                document_id=user_id
            )
            logger.info(f"Successfully fetched user activity for {user_id}")
            return result
        except AppwriteException as e:
            if e.code == 404:
                logger.info(f"No activity document found for user {user_id}")
                return None
            logger.error(f"Appwrite error fetching user activity for {user_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching user activity for {user_id}: {e}")
            return None
    
    def update_user_activity(self, user_id, job_id):
        """Update user activity with enhanced error handling and validation"""
        try:
            logger.info(f"Updating user activity: User {user_id}, Job {job_id}")
            
            # Validate inputs
            if not user_id or not job_id:
                logger.error(f"Invalid inputs: user_id={user_id}, job_id={job_id}")
                return None
            
            # Get existing activity
            existing_activity = self.get_user_activity(user_id)
            
            # Prepare recent activities list
            recent_activities = []
            
            if existing_activity:
                logger.info(f"Found existing activity document for user {user_id}")
                # Extract existing activities
                for i in range(1, 11):
                    activity_key = f'recent_activity_{i}' if i > 1 else 'recent_activity'
                    activity_value = existing_activity.get(activity_key, '0')
                    if activity_value and activity_value != '0':
                        recent_activities.append(activity_value)
                logger.info(f"Existing activities: {recent_activities}")
            else:
                logger.info(f"No existing activity document for user {user_id}")
            
            # Add new job_id at the beginning (remove duplicates)
            if job_id in recent_activities:
                recent_activities.remove(job_id)
            recent_activities.insert(0, job_id)
            
            # Keep only last 10 activities
            recent_activities = recent_activities[:10]
            logger.info(f"Updated activities list: {recent_activities}")
            
            # Prepare update data - CRITICAL: Include userId field
            update_data = {'userId': user_id}
            
            # Fill the recent_activity fields
            for i in range(10):
                activity_key = f'recent_activity_{i+1}' if i > 0 else 'recent_activity'
                if i < len(recent_activities):
                    update_data[activity_key] = recent_activities[i]
                else:
                    update_data[activity_key] = '0'
            
            logger.info(f"Prepared update data: {update_data}")
            
            if existing_activity:
                # Update existing document
                result = self.databases.update_document(
                    database_id=self.database_id,
                    collection_id=self.user_activity_collection_id,
                    document_id=user_id,
                    data=update_data
                )
                logger.info(f"Successfully updated activity document for user {user_id}")
            else:
                # Create new document
                result = self.databases.create_document(
                    database_id=self.database_id,
                    collection_id=self.user_activity_collection_id,
                    document_id=user_id,
                    data=update_data
                )
                logger.info(f"Successfully created activity document for user {user_id}")
            
            return result
                
        except AppwriteException as e:
            logger.error(f"Appwrite error updating user activity: {e}")
            logger.error(f"Error code: {e.code}")
            logger.error(f"Error message: {e.message}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error updating user activity: {e}")
            return None
    
    def get_user_recent_activities_with_jobs(self, user_id):
        """Get user's recent activities with full job data"""
        try:
            logger.info(f"Fetching recent activities with jobs for user {user_id}")
            activity_data = self.get_user_activity(user_id)
            if not activity_data:
                logger.info(f"No activity data found for user {user_id}")
                return []
            
            recent_jobs = []
            
            # Get job data for each recent activity
            for i in range(1, 11):
                activity_key = f'recent_activity_{i}' if i > 1 else 'recent_activity'
                job_id = activity_data.get(activity_key, '0')
                
                if job_id and job_id != '0':
                    job_data = self.get_job(job_id)
                    if job_data:
                        recent_jobs.append(job_data)
                    else:
                        logger.warning(f"Job {job_id} not found in database")
            
            logger.info(f"Found {len(recent_jobs)} recent jobs for user {user_id}")
            return recent_jobs
            
        except Exception as e:
            logger.error(f"Error fetching user recent activities with jobs: {e}")
            return []
    
    def test_user_activity_operations(self, test_user_id="test_user_123", test_job_id="test_job_456"):
        """Test function to verify user activity operations"""
        logger.info("=== TESTING USER ACTIVITY OPERATIONS ===")
        
        try:
            # Test 1: Update activity
            logger.info(f"Test 1: Updating activity for user {test_user_id} with job {test_job_id}")
            result = self.update_user_activity(test_user_id, test_job_id)
            if result:
                logger.info("✅ Activity update successful")
            else:
                logger.error("❌ Activity update failed")
            
            # Test 2: Fetch activity
            logger.info(f"Test 2: Fetching activity for user {test_user_id}")
            activity = self.get_user_activity(test_user_id)
            if activity:
                logger.info("✅ Activity fetch successful")
                logger.info(f"Activity data: {activity}")
            else:
                logger.error("❌ Activity fetch failed")
            
            # Test 3: Fetch recent activities with jobs
            logger.info(f"Test 3: Fetching recent activities with jobs for user {test_user_id}")
            recent_jobs = self.get_user_recent_activities_with_jobs(test_user_id)
            logger.info(f"✅ Found {len(recent_jobs)} recent jobs")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Test failed with error: {e}")
            return False