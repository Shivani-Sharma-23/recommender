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
            'personalized_jobs': '/api/get-personalized-jobs'
        }
    })

@app.route('/api/health', methods=['GET'])
def health_check():
    logger.info("Health check accessed")
    return jsonify({'status': 'healthy', 'message': 'Job Recommendation API is running'})

@app.route('/api/test', methods=['GET'])
def test_route():
    logger.info("Test route accessed")
    return jsonify({'success': True, 'message': 'Test route working!'})

@app.route('/api/user/<user_id>', methods=['GET'])
def get_user(user_id):
    try:
        user = appwrite_client.get_user(user_id)
        if user:
            return jsonify({'success': True, 'user': user})
        else:
            return jsonify({'success': False, 'message': 'User not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/recommendations/<user_id>', methods=['GET'])
def get_recommendations(user_id):
    try:
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
                    'jobId': job.get('jobId'),
                    'jobRole': job.get('jobRole'),
                    'companyName': job.get('companyName'),
                    'description': job.get('description', '')[:200] + '...',  # Truncate
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
                    'textSimilarity': round(rec['text_similarity'] * 100, 2)
                }
            })
        
        return jsonify({
            'success': True,
            'recommendations': formatted_recommendations,
            'total': len(formatted_recommendations),
            'userId': user_id
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

# NEW ENDPOINT - This matches your frontend call
@app.route('/api/get-personalized-jobs', methods=['POST'])
def get_personalized_jobs():
    try:
        data = request.get_json()
        
        # Extract parameters from request
        skills = data.get('skills', [])
        location = data.get('location', 'flexible')
        job_type = data.get('job_type', 'internship')
        experience_level = data.get('experience_level', 'entry')
        
        # Create a mock user profile for the recommendation engine
        mock_user_data = {
            'skills': skills,
            'location': location,
            'experienceLevel': experience_level,
            'education': '',  # Add if available
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
            if not job.get('jobId') or not job.get('jobRole'):
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
            
            # Text similarity
            job_text = recommendation_engine.extract_features_from_job(job)
            user_text = f"{' '.join(skills)} {experience_level}"
            
            try:
                from sklearn.feature_extraction.text import TfidfVectorizer
                from sklearn.metrics.pairwise import cosine_similarity
                
                vectorizer = TfidfVectorizer(stop_words='english', max_features=1000)
                corpus = [user_text.lower(), job_text]
                tfidf_matrix = vectorizer.fit_transform(corpus)
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
        
        # Sort by score
        job_scores.sort(key=lambda x: x['score'], reverse=True)
        
        # Format jobs for frontend (matching the expected format)
        formatted_jobs = []
        for rec in job_scores[:20]:  # Return top 20
            job = rec['job']
            formatted_jobs.append({
                'id': job.get('jobId', job.get('$id', '')),
                'title': job.get('jobRole', ''),
                'company': job.get('companyName', ''),
                'location': job.get('location', ''),
                'description': job.get('description', ''),
                'type': job.get('jobType', ''),
                'skills': job.get('skills', []),
                'salary': job.get('stipend', 'Salary not specified'),
                'apply_link': job.get('applyLink', '#'),
                'posted': 'Recently',  # You might want to add actual posting date
                'logo': f"https://logo.clearbit.com/{job.get('companyName', '').replace(' ', '').lower()}.com",
                'source': 'Gigrithm',
                'matchScore': round(rec['score'] * 100, 2)
            })
        
        return jsonify({
            'success': True,
            'jobs': formatted_jobs,
            'total': len(formatted_jobs)
        })
        
    except Exception as e:
        logger.error(f"Error in get_personalized_jobs: {str(e)}")
        return jsonify({'success': False, 'message': str(e), 'jobs': []}), 500

# NEW ENDPOINT - For general job search (matching your default job search)
@app.route('/api/search-jobs', methods=['GET'])
def search_jobs():
    try:
        # Get query parameters
        query = request.args.get('q', '')
        location = request.args.get('location', 'all')
        job_type = request.args.get('type', 'internship')
        limit = int(request.args.get('limit', 50))
        
        # Get jobs from database
        jobs_response = appwrite_client.get_jobs(limit=limit * 2)  # Get more to filter
        
        if not jobs_response or not jobs_response.get('documents'):
            return jsonify({
                'success': False,
                'message': 'No jobs found',
                'jobs': []
            })
        
        jobs = jobs_response['documents']
        filtered_jobs = []
        
        for job in jobs:
            if not job.get('jobId') or not job.get('jobRole'):
                continue
            
            # Filter by job type
            if job_type != 'all' and job.get('jobType', '').lower() != job_type.lower():
                continue
            
            # Filter by location
            if location != 'all' and location != 'flexible':
                job_location = job.get('location', '').lower()
                if location.lower() not in job_location and 'remote' not in job_location:
                    continue
            
            # Filter by search query
            if query:
                searchable_text = f"{job.get('jobRole', '')} {job.get('companyName', '')} {job.get('description', '')}".lower()
                if query.lower() not in searchable_text:
                    continue
            
            # Format job for frontend
            formatted_job = {
                'id': job.get('jobId', job.get('$id', '')),
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
                'source': 'Gigrithm'
            }
            
            filtered_jobs.append(formatted_job)
            
            if len(filtered_jobs) >= limit:
                break
        
        return jsonify({
            'success': True,
            'jobs': filtered_jobs,
            'total': len(filtered_jobs)
        })
        
    except Exception as e:
        logger.error(f"Error in search_jobs: {str(e)}")
        return jsonify({'success': False, 'message': str(e), 'jobs': []}), 500

@app.route('/api/jobs', methods=['GET'])
def get_jobs():
    try:
        limit = int(request.args.get('limit', 50))
        jobs_response = appwrite_client.get_jobs(limit)
        
        if jobs_response:
            return jsonify({
                'success': True,
                'jobs': jobs_response['documents'],
                'total': jobs_response['total']
            })
        else:
            return jsonify({'success': False, 'message': 'No jobs found'}), 404
            
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500
    
@app.route('/api')
def api_info():
    return jsonify({
        'api': 'Job Recommendation System',
        'version': '1.0',
        'endpoints': [
            {'path': '/api/health', 'method': 'GET', 'description': 'Health check'},
            {'path': '/api/jobs', 'method': 'GET', 'description': 'Get all jobs', 'params': 'limit (optional)'},
            {'path': '/api/search-jobs', 'method': 'GET', 'description': 'Search jobs', 'params': 'q, location, type, limit (all optional)'},
            {'path': '/api/user/<user_id>', 'method': 'GET', 'description': 'Get user by ID'},
            {'path': '/api/recommendations/<user_id>', 'method': 'GET', 'description': 'Get job recommendations', 'params': 'limit, jobType, location, category (all optional)'},
            {'path': '/api/get-personalized-jobs', 'method': 'POST', 'description': 'Get personalized job recommendations', 'body': 'skills, location, job_type, experience_level'}
        ]
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=False, host='0.0.0.0', port=port)