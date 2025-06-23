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
    
    def get_user_activity_job_ids(self, user_id):
        """Get all valid job IDs from user activity collection"""
        try:
            activity_data = self.client.get_user_activity(user_id)
            if not activity_data:
                return []
            
            valid_job_ids = []
            
            # Extract job IDs from all recent_activity fields (1-10)
            for i in range(1, 11):
                field_name = f'recent_activity_{i}' if i > 1 else 'recent_activity'
                job_id = activity_data.get(field_name, '0')
                
                # Only include valid job IDs (not '0' or empty)
                if job_id and job_id != '0' and job_id.strip():
                    valid_job_ids.append(job_id.strip())
            
            return valid_job_ids
            
        except Exception as e:
            print(f"Error fetching user activity job IDs: {e}")
            return []
    
    def get_jobs_from_activity(self, user_id):
        """Get actual job data from user activity - only valid jobs"""
        job_ids = self.get_user_activity_job_ids(user_id)
        if not job_ids:
            return []
        
        jobs_data = []
        for job_id in job_ids:
            try:
                job_data = self.client.get_job(job_id)
                if job_data and job_data.get('jobRole'):  # Ensure it's a valid job
                    jobs_data.append(job_data)
            except Exception as e:
                print(f"Error fetching job {job_id}: {e}")
                continue
        
        return jobs_data
    
    def analyze_user_preferences_from_activity(self, recent_jobs):
        """Analyze user preferences based on recent job clicks with weighted scoring"""
        if not recent_jobs:
            return {}
        
        # Weight recent activities more (first activity gets highest weight)
        weights = [1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1]
        
        # Extract patterns from recent activities with weights
        weighted_companies = {}
        weighted_job_types = {}
        weighted_locations = {}
        weighted_categories = {}
        weighted_skills = {}
        
        for i, job in enumerate(recent_jobs):
            weight = weights[i] if i < len(weights) else 0.1
            
            # Company preferences
            company = job.get('companyName', '').strip()
            if company:
                weighted_companies[company] = weighted_companies.get(company, 0) + weight
            
            # Job type preferences
            job_type = job.get('jobType', '').strip()
            if job_type:
                weighted_job_types[job_type] = weighted_job_types.get(job_type, 0) + weight
            
            # Location preferences
            location = job.get('location', '').strip()
            if location:
                weighted_locations[location] = weighted_locations.get(location, 0) + weight
            
            # Category preferences
            category = job.get('category', '').strip()
            if category:
                weighted_categories[category] = weighted_categories.get(category, 0) + weight
            
            # Skills preferences
            job_skills = job.get('skills', [])
            if isinstance(job_skills, list):
                for skill in job_skills:
                    if skill and skill.strip():
                        skill_clean = skill.lower().strip()
                        weighted_skills[skill_clean] = weighted_skills.get(skill_clean, 0) + weight
            elif isinstance(job_skills, str):
                for skill in job_skills.split(','):
                    if skill and skill.strip():
                        skill_clean = skill.lower().strip()
                        weighted_skills[skill_clean] = weighted_skills.get(skill_clean, 0) + weight
        
        # Sort by weight and return top preferences
        preferences = {
            'preferred_companies': sorted(weighted_companies.items(), key=lambda x: x[1], reverse=True)[:5],
            'preferred_job_types': sorted(weighted_job_types.items(), key=lambda x: x[1], reverse=True)[:3],
            'preferred_locations': sorted(weighted_locations.items(), key=lambda x: x[1], reverse=True)[:3],
            'preferred_categories': sorted(weighted_categories.items(), key=lambda x: x[1], reverse=True)[:3],
            'trending_skills': sorted(weighted_skills.items(), key=lambda x: x[1], reverse=True)[:10]
        }
        
        return preferences
    
    def calculate_activity_based_score(self, job, user_preferences, recent_job_ids):
        """Enhanced activity-based scoring with decay and similarity"""
        if not user_preferences:
            return 0.0
        
        score = 0.0
        
        # Avoid recommending recently clicked jobs
        job_id = job.get('$id') or job.get('jobId')
        if job_id in recent_job_ids:
            return -1.0  # Negative score to filter out
        
        # Company preference with weighted scoring
        job_company = job.get('companyName', '').strip().lower()
        preferred_companies = [(comp.lower(), weight) for comp, weight in user_preferences.get('preferred_companies', [])]
        for pref_company, weight in preferred_companies:
            if job_company == pref_company:
                score += 0.25 * min(weight, 1.0)  # Cap weight at 1.0
                break
            elif pref_company in job_company or job_company in pref_company:
                score += 0.15 * min(weight, 1.0)  # Partial match
                break
        
        # Job type preference
        job_type = job.get('jobType', '').strip().lower()
        preferred_types = [(jtype.lower(), weight) for jtype, weight in user_preferences.get('preferred_job_types', [])]
        for pref_type, weight in preferred_types:
            if job_type == pref_type:
                score += 0.2 * min(weight, 1.0)
                break
        
        # Location preference
        job_location = job.get('location', '').strip().lower()
        preferred_locations = [(loc.lower(), weight) for loc, weight in user_preferences.get('preferred_locations', [])]
        for pref_location, weight in preferred_locations:
            if pref_location in job_location or job_location in pref_location:
                score += 0.15 * min(weight, 1.0)
                break
        
        # Category preference
        job_category = job.get('category', '').strip().lower()
        preferred_categories = [(cat.lower(), weight) for cat, weight in user_preferences.get('preferred_categories', [])]
        for pref_category, weight in preferred_categories:
            if job_category == pref_category:
                score += 0.15 * min(weight, 1.0)
                break
        
        # Skills alignment with trending interests
        job_skills = set(self.preprocess_skills(job.get('skills', [])))
        trending_skills = dict(user_preferences.get('trending_skills', []))
        
        skill_score = 0.0
        for job_skill in job_skills:
            if job_skill in trending_skills:
                skill_score += trending_skills[job_skill]
        
        # Normalize skill score
        if job_skills:
            skill_score = min(skill_score / len(job_skills), 1.0)
            score += 0.25 * skill_score
        
        return min(score, 1.0)  # Cap at 1.0
    
    def calculate_content_similarity(self, user_profile, job):
        """Calculate content-based similarity between user profile and job"""
        try:
            # Build user profile text
            user_skills = user_profile.get('skills', [])
            user_education = user_profile.get('education', '')
            user_experience = user_profile.get('experienceLevel', '')
            
            user_text = f"{' '.join(user_skills)} {user_education} {user_experience}".strip()
            
            # Build job text
            job_text = self.extract_features_from_job(job)
            
            if not user_text or not job_text:
                return 0.0
            
            # Calculate TF-IDF similarity
            corpus = [user_text.lower(), job_text]
            tfidf_matrix = self.tfidf_vectorizer.fit_transform(corpus)
            similarity = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:2])[0][0]
            
            return similarity
            
        except Exception as e:
            print(f"Error calculating content similarity: {e}")
            return 0.0
    
    def get_recommendations(self, user_id: str, num_recommendations: int = 10):
        """Generate enhanced job recommendations using activity-based collaborative filtering"""
        print(f"Generating recommendations for user: {user_id}")
        
        # Get user data
        user_data = self.client.get_user(user_id)
        if not user_data:
            print(f"User {user_id} not found")
            return []
        
        # Get user's recent activities from user_activity collection
        recent_jobs = self.get_jobs_from_activity(user_id)
        recent_job_ids = self.get_user_activity_job_ids(user_id)
        
        print(f"Found {len(recent_jobs)} valid recent job activities for user {user_id}")
        print(f"Recent job IDs: {recent_job_ids}")
        
        # Analyze user preferences from activity
        user_preferences = self.analyze_user_preferences_from_activity(recent_jobs)
        
        # Get all available jobs
        jobs_response = self.client.get_jobs(limit=500)
        if not jobs_response or not jobs_response['documents']:
            print("No jobs found in database")
            return []
        
        jobs = jobs_response['documents']
        print(f"Processing {len(jobs)} total jobs")
        
        # Extract user features
        user_skills = user_data.get('skills', [])
        user_location = user_data.get('location', '')
        user_experience = user_data.get('experienceLevel', '')
        
        # Calculate scores for each job
        job_scores = []
        
        for job in jobs:
            # Skip if job doesn't have required fields
            if not job.get('jobId') or not job.get('jobRole'):
                continue
            
            job_id = job.get('$id') or job.get('jobId')
            
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
            
            # Enhanced activity-based preference score
            activity_score = self.calculate_activity_based_score(
                job, user_preferences, recent_job_ids
            )
            
            # Skip recently clicked jobs (negative activity score)
            if activity_score < 0:
                continue
            
            # Content-based similarity
            content_similarity = self.calculate_content_similarity(user_data, job)
            
            # Enhanced weighted final score with activity emphasis
            if recent_jobs:  # If user has activity history
                final_score = (
                    skill_score * 0.2 +           # Reduced individual factors
                    content_similarity * 0.2 +    # when activity data is available
                    location_score * 0.1 +        
                    experience_score * 0.1 +      
                    activity_score * 0.4          # Heavy emphasis on activity patterns
                )
            else:  # If no activity history, use traditional content-based approach
                final_score = (
                    skill_score * 0.35 +
                    content_similarity * 0.35 +
                    location_score * 0.2 +
                    experience_score * 0.1
                )
            
            job_scores.append({
                'job': job,
                'score': final_score,
                'skill_score': skill_score,
                'location_score': location_score,
                'experience_score': experience_score,
                'content_similarity': content_similarity,
                'activity_score': activity_score,
                'has_activity_data': len(recent_jobs) > 0,
                'recommendation_reason': self.get_recommendation_reason(
                    skill_score, activity_score, content_similarity, len(recent_jobs) > 0
                )
            })
        
        # Sort by score and return top recommendations
        job_scores.sort(key=lambda x: x['score'], reverse=True)
        
        # Debug information
        if recent_jobs:
            print(f"\nUser {user_id} activity-based preferences:")
            print(f"Top companies: {[comp for comp, _ in user_preferences.get('preferred_companies', [])[:3]]}")
            print(f"Top job types: {[jtype for jtype, _ in user_preferences.get('preferred_job_types', [])[:3]]}")
            print(f"Top skills: {[skill for skill, _ in user_preferences.get('trending_skills', [])[:5]]}")
            print(f"Recent job IDs filtered: {recent_job_ids[:5]}")
        else:
            print(f"No activity data found for user {user_id}, using content-based recommendations")
        
        return job_scores[:num_recommendations]
    
    def get_recommendation_reason(self, skill_score, activity_score, content_similarity, has_activity):
        """Generate human-readable recommendation reason"""
        if has_activity and activity_score > 0.3:
            return "Based on your recent job interests"
        elif skill_score > 0.4:
            return "Strong skill match with your profile"
        elif content_similarity > 0.3:
            return "Similar to your profile and preferences"
        else:
            return "Recommended for you"
    
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
        """Get comprehensive insights about user's job search behavior"""
        recent_jobs = self.get_jobs_from_activity(user_id)
        if not recent_jobs:
            return None
        
        preferences = self.analyze_user_preferences_from_activity(recent_jobs)
        
        # Calculate activity trends
        activity_depth = len([job for job in recent_jobs if job])
        recent_companies = list(set([job.get('companyName') for job in recent_jobs[-5:] if job.get('companyName')]))
        
        insights = {
            'total_activities': activity_depth,
            'activity_strength': 'high' if activity_depth >= 7 else 'moderate' if activity_depth >= 3 else 'low',
            'top_interests': {
                'companies': [comp for comp, _ in preferences.get('preferred_companies', [])[:3]],
                'job_types': [jtype for jtype, _ in preferences.get('preferred_job_types', [])[:3]],
                'locations': [loc for loc, _ in preferences.get('preferred_locations', [])[:3]],
                'skills': [skill for skill, _ in preferences.get('trending_skills', [])[:5]]
            },
            'recent_companies': recent_companies,
            'activity_trend': 'increasing' if activity_depth >= 5 else 'moderate',
            'personalization_level': 'high' if activity_depth >= 5 else 'developing'
        }
        
        return insights