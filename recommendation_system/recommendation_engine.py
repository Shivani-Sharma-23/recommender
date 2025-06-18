import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import MultiLabelBinarizer
import re
from typing import List, Dict, Any

class JobRecommendationEngine:
    def __init__(self, appwrite_client):
        self.client = appwrite_client
        self.tfidf_vectorizer = TfidfVectorizer(stop_words='english', max_features=1000)
        self.mlb_skills = MultiLabelBinarizer()
        
    def preprocess_skills(self, skills):
        """Convert skills to a standardized format"""
        if isinstance(skills, str):
            skills = [s.strip().lower() for s in skills.split(',')]
        elif isinstance(skills, list):
            skills = [s.strip().lower() for s in skills if s]
        else:
            skills = []
        return skills
    
    def extract_features_from_job(self, job):
        """Extract and combine features from job data"""
        description = job.get('description', '')
        job_role = job.get('jobRole', '')
        company = job.get('companyName', '')
        category = job.get('category', '')
        
        # Combine text features
        text_features = f"{job_role} {description} {company} {category}"
        return text_features.lower()
    
    def calculate_skill_similarity(self, user_skills, job_skills):
        """Calculate similarity between user skills and job requirements"""
        user_skills_set = set(self.preprocess_skills(user_skills))
        job_skills_set = set(self.preprocess_skills(job_skills))
        
        if not user_skills_set or not job_skills_set:
            return 0.0
        
        intersection = user_skills_set.intersection(job_skills_set)
        union = user_skills_set.union(job_skills_set)
        
        return len(intersection) / len(union) if union else 0.0
    
    def calculate_location_match(self, user_location, job_location):
        """Calculate location compatibility"""
        if not user_location or not job_location:
            return 0.5  # neutral score
        
        user_location = user_location.lower().strip()
        job_location = job_location.lower().strip()
        
        if user_location == job_location:
            return 1.0
        elif 'remote' in job_location or 'anywhere' in job_location:
            return 0.9
        elif any(word in job_location for word in user_location.split()):
            return 0.7
        else:
            return 0.3
    
    def calculate_experience_match(self, user_experience, job_experience):
        """Calculate experience level compatibility"""
        experience_levels = {
            'entry': 1, 'junior': 1, 'fresher': 1,
            'mid': 2, 'intermediate': 2, 'senior': 3,
            'lead': 4, 'principal': 5, 'director': 6
        }
        
        user_level = 1
        job_level = 1
        
        if user_experience:
            for level, value in experience_levels.items():
                if level in user_experience.lower():
                    user_level = value
                    break
        
        if job_experience:
            for level, value in experience_levels.items():
                if level in job_experience.lower():
                    job_level = value
                    break
        
        # Calculate compatibility score
        diff = abs(user_level - job_level)
        return max(0, 1 - (diff * 0.2))
    
    def get_recommendations(self, user_id: str, num_recommendations: int = 10):
        """Generate job recommendations for a user"""
        # Get user data
        user_data = self.client.get_user(user_id)
        if not user_data:
            return []
        
        # Get all jobs
        jobs_response = self.client.get_jobs(limit=500)
        if not jobs_response or not jobs_response['documents']:
            return []
        
        jobs = jobs_response['documents']
        
        # Extract user features
        user_skills = user_data.get('skills', [])
        user_location = user_data.get('location', '')
        user_experience = user_data.get('experienceLevel', '')
        user_education = user_data.get('education', '')
        
        # Calculate scores for each job
        job_scores = []
        
        for job in jobs:
            # Skip if job doesn't have required fields
            if not job.get('jobId') or not job.get('jobRole'):
                continue
            
            # Calculate individual similarity scores
            skill_score = self.calculate_skill_similarity(
                user_skills, job.get('skills', [])
            )
            
            location_score = self.calculate_location_match(
                user_location, job.get('location', '')
            )
            
            experience_score = self.calculate_experience_match(
                user_experience, job.get('experienceLevel', '')
            )
            
            # Text similarity using TF-IDF
            job_text = self.extract_features_from_job(job)
            user_text = f"{' '.join(user_skills)} {user_education} {user_experience}"
            
            try:
                # Create corpus for TF-IDF
                corpus = [user_text.lower(), job_text]
                tfidf_matrix = self.tfidf_vectorizer.fit_transform(corpus)
                text_similarity = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:2])[0][0]
            except:
                text_similarity = 0.0
            
            # Weighted final score
            final_score = (
                skill_score * 0.4 +
                text_similarity * 0.3 +
                location_score * 0.2 +
                experience_score * 0.1
            )
            
            job_scores.append({
                'job': job,
                'score': final_score,
                'skill_score': skill_score,
                'location_score': location_score,
                'experience_score': experience_score,
                'text_similarity': text_similarity
            })
        
        # Sort by score and return top recommendations
        job_scores.sort(key=lambda x: x['score'], reverse=True)
        print(job_scores[:num_recommendations])
        return job_scores[:num_recommendations]
    
    def get_filtered_recommendations(self, user_id: str, filters: Dict = None, num_recommendations: int = 10):
        """Get recommendations with additional filters"""
        recommendations = self.get_recommendations(user_id, num_recommendations * 2)
        
        if not filters:
            return recommendations[:num_recommendations]
        
        filtered_recommendations = []
        for rec in recommendations:
            job = rec['job']
            
            # Apply filters
            if filters.get('jobType') and job.get('jobType') != filters['jobType']:
                continue
            if filters.get('location') and filters['location'].lower() not in job.get('location', '').lower():
                continue
            if filters.get('category') and job.get('category') != filters['category']:
                continue
            
            filtered_recommendations.append(rec)
            
            if len(filtered_recommendations) >= num_recommendations:
                break
        
        return filtered_recommendations