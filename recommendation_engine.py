import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import MultiLabelBinarizer
import re
from typing import List, Dict, Any
from collections import Counter

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
    
    def analyze_user_preferences_from_activity(self, recent_jobs):
        """Analyze user preferences based on recent job clicks"""
        if not recent_jobs:
            return {}
        
        # Extract patterns from recent activities
        companies = [job.get('companyName', '') for job in recent_jobs if job.get('companyName')]
        job_types = [job.get('jobType', '') for job in recent_jobs if job.get('jobType')]
        locations = [job.get('location', '') for job in recent_jobs if job.get('location')]
        categories = [job.get('category', '') for job in recent_jobs if job.get('category')]
        
        # Extract skills from job descriptions and requirements
        all_skills = []
        for job in recent_jobs:
            job_skills = job.get('skills', [])
            if isinstance(job_skills, list):
                all_skills.extend([skill.lower().strip() for skill in job_skills])
            elif isinstance(job_skills, str):
                all_skills.extend([skill.lower().strip() for skill in job_skills.split(',')])
        
        # Calculate preferences based on frequency
        preferences = {
            'preferred_companies': Counter(companies).most_common(3),
            'preferred_job_types': Counter(job_types).most_common(3),
            'preferred_locations': Counter(locations).most_common(3),
            'preferred_categories': Counter(categories).most_common(3),
            'trending_skills': Counter(all_skills).most_common(10)
        }
        
        return preferences
    
    def calculate_activity_based_score(self, job, user_preferences):
        """Calculate score based on user's activity patterns"""
        if not user_preferences:
            return 0.0
        
        score = 0.0
        
        # Company preference
        job_company = job.get('companyName', '').lower()
        preferred_companies = [comp.lower() for comp, _ in user_preferences.get('preferred_companies', [])]
        if job_company in preferred_companies:
            # Higher score for more frequently clicked companies
            company_rank = preferred_companies.index(job_company)
            score += 0.3 * (1 - company_rank * 0.2)  # 0.3, 0.24, 0.18
        
        # Job type preference
        job_type = job.get('jobType', '').lower()
        preferred_types = [jtype.lower() for jtype, _ in user_preferences.get('preferred_job_types', [])]
        if job_type in preferred_types:
            type_rank = preferred_types.index(job_type)
            score += 0.2 * (1 - type_rank * 0.2)
        
        # Location preference
        job_location = job.get('location', '').lower()
        preferred_locations = [loc.lower() for loc, _ in user_preferences.get('preferred_locations', [])]
        for pref_loc in preferred_locations:
            if pref_loc in job_location or job_location in pref_loc:
                loc_rank = preferred_locations.index(pref_loc)
                score += 0.15 * (1 - loc_rank * 0.2)
                break
        
        # Category preference
        job_category = job.get('category', '').lower()
        preferred_categories = [cat.lower() for cat, _ in user_preferences.get('preferred_categories', [])]
        if job_category in preferred_categories:
            cat_rank = preferred_categories.index(job_category)
            score += 0.15 * (1 - cat_rank * 0.2)
        
        # Skills alignment with trending interests
        job_skills = self.preprocess_skills(job.get('skills', []))
        trending_skills = [skill for skill, _ in user_preferences.get('trending_skills', [])]
        
        skill_matches = len(set(job_skills).intersection(set(trending_skills)))
        if skill_matches > 0:
            score += 0.2 * min(skill_matches / 5, 1.0)  # Cap at 5 matching skills
        
        return min(score, 1.0)  # Cap at 1.0
    
    def get_recommendations(self, user_id: str, num_recommendations: int = 10):
        """Generate job recommendations for a user with activity-based enhancement"""
        # Get user data
        user_data = self.client.get_user(user_id)
        if not user_data:
            return []
        
        # Get user's recent activities
        recent_jobs = self.client.get_user_recent_activities_with_jobs(user_id)
        user_preferences = self.analyze_user_preferences_from_activity(recent_jobs)
        
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
        
        # Get IDs of recently clicked jobs to avoid recommending them again
        recent_job_ids = [job.get('$id') for job in recent_jobs if job.get('$id')]
        
        # Calculate scores for each job
        job_scores = []
        
        for job in jobs:
            # Skip if job doesn't have required fields or was recently clicked
            if not job.get('jobId') or not job.get('jobRole'):
                continue
            
            job_id = job.get('$id') or job.get('jobId')
            if job_id in recent_job_ids:
                continue  # Skip recently clicked jobs
            
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
            
            # Activity-based preference score
            activity_score = self.calculate_activity_based_score(job, user_preferences)
            
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
            
            # Enhanced weighted final score including activity patterns
            final_score = (
                skill_score * 0.25 +           # Reduced from 0.4
                text_similarity * 0.25 +       # Reduced from 0.3
                location_score * 0.15 +        # Reduced from 0.2
                experience_score * 0.1 +       # Reduced from 0.1
                activity_score * 0.25          # New: User activity-based score
            )
            
            job_scores.append({
                'job': job,
                'score': final_score,
                'skill_score': skill_score,
                'location_score': location_score,
                'experience_score': experience_score,
                'text_similarity': text_similarity,
                'activity_score': activity_score,
                'is_similar_to_recent': activity_score > 0.3  # Flag for UI
            })
        
        # Sort by score and return top recommendations
        job_scores.sort(key=lambda x: x['score'], reverse=True)
        
        # Add debug info about user preferences
        if recent_jobs:
            print(f"User {user_id} preferences based on {len(recent_jobs)} recent activities:")
            print(f"Top companies: {user_preferences.get('preferred_companies', [])[:3]}")
            print(f"Top job types: {user_preferences.get('preferred_job_types', [])[:3]}")
            print(f"Top skills: {user_preferences.get('trending_skills', [])[:5]}")
        
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
    
    def get_user_activity_insights(self, user_id: str):
        """Get insights about user's job search behavior"""
        recent_jobs = self.client.get_user_recent_activities_with_jobs(user_id)
        if not recent_jobs:
            return None
        
        preferences = self.analyze_user_preferences_from_activity(recent_jobs)
        
        insights = {
            'total_activities': len(recent_jobs),
            'top_interests': {
                'companies': [comp for comp, count in preferences.get('preferred_companies', [])[:3]],
                'job_types': [jtype for jtype, count in preferences.get('preferred_job_types', [])[:3]],
                'locations': [loc for loc, count in preferences.get('preferred_locations', [])[:3]],
                'skills': [skill for skill, count in preferences.get('trending_skills', [])[:5]]
            },
            'recent_companies': list(set([job.get('companyName') for job in recent_jobs[-5:] if job.get('companyName')])),
            'activity_trend': 'increasing' if len(recent_jobs) >= 5 else 'moderate'
        }
        
        return insights