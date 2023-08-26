import jwt
from flask import request
from functools import wraps
from .config import SECRET_KEY

def employee_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('token')
        if not token:
            return {'message': 'Token is missing!'}
        
        try:
            data = jwt.decode(token, SECRET_KEY, 'HS256')
        except:
            return {'message': 'Token is invalid!'}

        return f(*args, **kwargs)

    return decorated