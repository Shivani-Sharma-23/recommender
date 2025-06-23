from flask import Flask, request, jsonify
from flask_cors import CORS
from appwrite_client import AppwriteClient
from recommendation_engine import JobRecommendationEngine
import os
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app, origins=['*'])  # Allow all origins for deployment

# Initialize clients
appwrite_client = AppwriteClient()
recommendation_engine = JobRecommendationEngine(appwrite_client)

@app.route('/')
def home():
    logger.info("Home route accessed")
    return jsonify({
        'message': 'Job Recommendation API',
        'status': 'running',
        'endpoints': {
            'health': '/api/health',
            'test': '/api/test',
            'recommendations': '/api/recommendations/<user_id>',
            'personalized_jobs': '/api/get-personalized-jobs',
            'search_jobs': '/api/search-jobs',
            'user_activity_insights': '/api/user-activity-insights/<user_id>',
            'track_activity': '/api/track-activity'
        }
    })

@app.route('/api/health', methods=['GET'])
def health_check():
    logger.info("Health check accessed")
    return jsonify({'status': 'healthy', 'message': 'Job Recommendation API is running'})

@app.route('/api/test', methods=['GET'])
def test_route():
    logger.info("Test route accessed")
    # Test Appwrite connection
    is_connected, message = appwrite_client.test_connection()
    return jsonify({
        'success': True, 
        'message': 'Test route working!',
        'appwrite_connection': is_connected,
        'appwrite_message': message
    })

@app.route('/api/user/<user_id>', methods=['GET'])
def get_user(user_id):
    try:
        user = appwrite_client.get_user(user_id)
        if user:
            return jsonify({'success': True, 'user': user})
        else:
            return jsonify({'success': False, 'message': 'User not found'}), 404
    except Exception as e:
        logger.error(f"Error in get_user: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/recommendations/<user_id>', methods=['GET'])
def get_recommendations(user_id):
    try:
        logger.info(f"Getting recommendations for user: {user_id}")
        
        # Get query parameters
        num_recommendations = int(request.args.get('limit', 10))
        job_type = request.args.get('jobType')
        location = request.args.get('location')
        category = request.args.get('category')
        
        # Build filters
        filters = {}
        if job_type:
            filters['jobType'] = job_type
        if location:
            filters['location'] = location
        if category:
            filters['category'] = category
        
        # Get recommendations
        if filters:
            recommendations = recommendation_engine.get_filtered_recommendations(
                user_id, filters, num_recommendations
            )
        else:
            recommendations = recommendation_engine.get_recommendations(
                user_id, num_recommendations
            )
        
        # Format response
        formatted_recommendations = []
        for rec in recommendations:
            job = rec['job']
            formatted_recommendations.append({
                'job': {
                    'jobId': job.get('jobId') or job.get('$id'),
                    'jobRole': job.get('jobRole'),
                    'companyName': job.get('companyName'),
                    'description': job.get('description', '')[:200] + '...' if job.get('description') else '',
                    'location': job.get('location'),
                    'jobType': job.get('jobType'),
                    'experienceLevel': job.get('experienceLevel'),
                    'skills': job.get('skills', []),
                    'applyLink': job.get('applyLink'),
                    'stipend': job.get('stipend'),
                    'category': job.get('category')
                },
                'matchScore': round(rec['score'] * 100, 2),
                'matchBreakdown': {
                    'skillMatch': round(rec['skill_score'] * 100, 2),
                    'locationMatch': round(rec['location_score'] * 100, 2),
                    'experienceMatch': round(rec['experience_score'] * 100, 2),
                    'contentSimilarity': round(rec['content_similarity'] * 100, 2),
                    'activityScore': round(rec['activity_score'] * 100, 2)
                },
                'recommendationReason': rec['recommendation_reason'],
                'hasActivityData': rec['has_activity_data']
            })
        
        return jsonify({
            'success': True,
            'recommendations': formatted_recommendations,
            'total': len(formatted_recommendations),
            'userId': user_id
        })
        
    except Exception as e:
        logger.error(f"Error in get_recommendations: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/get-personalized-jobs', methods=['POST'])
def get_personalized_jobs():
    try:
        data = request.get_json()
        logger.info(f"Getting personalized jobs with data: {data}")
        
        # Extract parameters from request
        user_id = data.get('user_id')  # If user is logged in
        skills = data.get('skills', [])
        location = data.get('location', 'flexible')
        job_type = data.get('job_type', 'internship')
        experience_level = data.get('experience_level', 'entry')
        
        # If user_id provided, use enhanced recommendation system
        if user_id:
            recommendations = recommendation_engine.get_recommendations(user_id, 20)
            
            # Format jobs for frontend
            formatted_jobs = []
            for rec in recommendations:
                job = rec['job']
                formatted_jobs.append({
                    'id': job.get('jobId') or job.get('$id', ''),
                    'title': job.get('jobRole', ''),
                    'company': job.get('companyName', ''),
                    'location': job.get('location', ''),
                    'description': job.get('description', ''),
                    'type': job.get('jobType', ''),
                    'skills': job.get('skills', []),
                    'salary': job.get('stipend', 'Salary not specified'),
                    'apply_link': job.get('applyLink', '#'),
                    'posted': 'Recently',
                    'logo': f"https://logo.clearbit.com/{job.get('companyName', '').replace(' ', '').lower()}.com",
                    'source': 'Gigrithm',
                    'matchScore': round(rec['score'] * 100, 2),
                    'recommendationReason': rec['recommendation_reason']
                })
            
            return jsonify({
                'success': True,
                'jobs': formatted_jobs,
                'total': len(formatted_jobs),
                'personalized': True
            })
        
        # If no user_id, use basic filtering
        else:
            # Create a mock user profile for the recommendation engine
            mock_user_data = {
                'skills': skills,
                'location': location,
                'experienceLevel': experience_level,
                'education': '',
            }
            
            # Get all jobs first
            jobs_response = appwrite_client.get_jobs(limit=500)
            if not jobs_response or not jobs_response.get('documents'):
                return jsonify({
                    'success': False,
                    'message': 'No jobs found',
                    'jobs': []
                })
            
            jobs = jobs_response['documents']
            
            # Calculate scores for each job using the recommendation engine logic
            job_scores = []
            
            for job in jobs:
                if not job.get('jobId') and not job.get('$id'):
                    continue
                if not job.get('jobRole'):
                    continue
                
                # Filter by job type if specified
                if job_type != 'all' and job.get('jobType', '').lower() != job_type.lower():
                    continue
                
                # Calculate similarity scores
                skill_score = recommendation_engine.calculate_skill_similarity(
                    skills, job.get('skills', [])
                )
                
                location_score = recommendation_engine.calculate_location_match(
                    location, job.get('location', '')
                )
                
                experience_score = recommendation_engine.calculate_experience_match(
                    experience_level, job.get('experienceLevel', '')
                )
                
                # Content similarity
                content_similarity = recommendation_engine.calculate_content_similarity(mock_user_data, job)
                
                # Weighted final score
                final_score = (
                    skill_score * 0.4 +
                    content_similarity * 0.3 +
                    location_score * 0.2 +
                    experience_score * 0.1
                )
                
                job_scores.append({
                    'job': job,
                    'score': final_score,
                    'skill_score': skill_score,
                    'location_score': location_score,
                    'experience_score': experience_score,
                    'content_similarity': content_similarity
                })
            
            # Sort by score
            job_scores.sort(key=lambda x: x['score'], reverse=True)
            
            # Format jobs for frontend
            formatted_jobs = []
            for rec in job_scores[:20]:  # Return top 20
                job = rec['job']
                formatted_jobs.append({
                    'id': job.get('jobId') or job.get('$id', ''),
                    'title': job.get('jobRole', ''),
                    'company': job.get('companyName', ''),
                    'location': job.get('location', ''),
                    'description': job.get('description', ''),
                    'type': job.get('jobType', ''),
                    'skills': job.get('skills', []),
                    'salary': job.get('stipend', 'Salary not specified'),
                    'apply_link': job.get('applyLink', '#'),
                    'posted': 'Recently',
                    'logo': f"https://logo.clearbit.com/{job.get('companyName', '').replace(' ', '').lower()}.com",
                    'source': 'Gigrithm',
                    'matchScore': round(rec['score'] * 100, 2)
                })
            
            return jsonify({
                'success': True,
                'jobs': formatted_jobs,
                'total': len(formatted_jobs),
                'personalized': False
            })
        
    except Exception as e:
        logger.error(f"Error in get_personalized_jobs: {str(e)}")
        return jsonify({'success': False, 'message': str(e), 'jobs': []}), 500

@app.route('/api/search-jobs', methods=['GET'])
def search_jobs():
    try:
        # Get query parameters
        query = request.args.get('q', '')
        location = request.args.get('location', 'all')
        job_type = request.args.get('type', 'all')
        experience_level = request.args.get('experience', 'all')
        limit = int(request.args.get('limit', 50))
        
        logger.info(f"Searching jobs with query: {query}, location: {location}, type: {job_type}")
        
        # Get jobs from database with filters
        if location != 'all' or job_type != 'all' or experience_level != 'all':
            jobs_response = appwrite_client.get_jobs_by_filters(
                location=location if location != 'all' else None,
                job_type=job_type if job_type != 'all' else None,
                experience_level=experience_level if experience_level != 'all' else None
            )
        else:
            jobs_response = appwrite_client.get_jobs(limit=limit * 2)
        
        if not jobs_response or not jobs_response.get('documents'):
            return jsonify({
                'success': False,
                'message': 'No jobs found',
                'jobs': []
            })
        
        jobs = jobs_response['documents']
        filtered_jobs = []
        
        for job in jobs:
            if not job.get('jobId') and not job.get('$id'):
                continue
            if not job.get('jobRole'):
                continue
            
            # Text search in job role, company name, and description
            if query:
                job_text = f"{job.get('jobRole', '')} {job.get('companyName', '')} {job.get('description', '')}".lower()
                if query.lower() not in job_text:
                    continue
            
            # Format job for response
            formatted_job = {
                'id': job.get('jobId') or job.get('$id', ''),
                'title': job.get('jobRole', ''),
                'company': job.get('companyName', ''),
                'location': job.get('location', ''),
                'description': job.get('description', ''),
                'type': job.get('jobType', ''),
                'experienceLevel': job.get('experienceLevel', ''),
                'skills': job.get('skills', []),
                'salary': job.get('stipend', 'Salary not specified'),
                'apply_link': job.get('applyLink', '#'),
                'posted': 'Recently',
                'logo': f"https://logo.clearbit.com/{job.get('companyName', '').replace(' ', '').lower()}.com",
                'source': 'Gigrithm',
                'category': job.get('category', '')
            }
            
            filtered_jobs.append(formatted_job)
            
            if len(filtered_jobs) >= limit:
                break
        
        return jsonify({
            'success': True,
            'jobs': filtered_jobs,
            'total': len(filtered_jobs),
            'query': query
        })
        
    except Exception as e:
        logger.error(f"Error in search_jobs: {str(e)}")
        return jsonify({'success': False, 'message': str(e), 'jobs': []}), 500

@app.route('/api/user-activity-insights/<user_id>', methods=['GET'])
def get_user_activity_insights(user_id):
    try:
        insights = recommendation_engine.get_user_activity_insights(user_id)
        
        if insights:
            return jsonify({
                'success': True,
                'insights': insights,
                'userId': user_id
            })
        else:
            return jsonify({
                'success': True,
                'insights': None,
                'message': 'No activity data found for user',
                'userId': user_id
            })
            
    except Exception as e:
        logger.error(f"Error in get_user_activity_insights: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/track-activity', methods=['POST'])
def track_user_activity():
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        job_id = data.get('job_id')
        
        if not user_id or not job_id:
            return jsonify({
                'success': False,
                'message': 'user_id and job_id are required'
            }), 400
        
        # Update user activity
        result = appwrite_client.update_user_activity(user_id, job_id)
        
        if result:
            return jsonify({
                'success': True,
                'message': 'Activity tracked successfully'
            })
        else:
            return jsonify({
                'success': False,
                'message': 'Failed to track activity'
            }), 500
            
    except Exception as e:
        logger.error(f"Error in track_user_activity: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/debug/user-activity/<user_id>', methods=['GET'])
def debug_user_activity(user_id):
    """Debug endpoint to check user activity data"""
    try:
        # Get user activity
        activity_data = appwrite_client.get_user_activity(user_id)
        
        # Get job IDs from activity
        job_ids = recommendation_engine.get_user_activity_job_ids(user_id)
        
        # Get actual jobs from activity
        recent_jobs = recommendation_engine.get_jobs_from_activity(user_id)
        
        # Get user preferences
        user_preferences = recommendation_engine.analyze_user_preferences_from_activity(recent_jobs)
        
        return jsonify({
            'success': True,
            'debug_info': {
                'activity_data': activity_data,
                'job_ids_found': job_ids,
                'valid_jobs_count': len(recent_jobs),
                'user_preferences': user_preferences,
                'recent_jobs_sample': [
                    {
                        'jobId': job.get('jobId'),
                        'jobRole': job.get('jobRole'),
                        'companyName': job.get('companyName')
                    } for job in recent_jobs[:3]
                ]
            }
        })
        
    except Exception as e:
        logger.error(f"Error in debug_user_activity: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)