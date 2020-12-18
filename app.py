import os
import boto3

from flask import Flask, jsonify, request
app = Flask(__name__)

USERS_TABLE = os.environ['USERS_TABLE']
REGION = os.environ['REGION']
IS_OFFLINE = os.environ['IS_OFFLINE']

if IS_OFFLINE:
    db = boto3.client(
        'dynamodb',
        region_name='localhost',
        endpoint_url='http://localhost:8000'
    )
else:
    db = boto3.client('dynamodb', region_name=REGION)

def item_to_hash(item):
    return {
        'userId': item.get('userId').get('S'),
        'name': item.get('name').get('S')
    }

@app.route("/")
def hello():
    return "Hello World"

@app.route("/users", methods=["GET"])
def get_all_users():
    paginator = db.get_paginator('scan')
    pages = paginator.paginate(
        TableName=USERS_TABLE,
        Select='ALL_ATTRIBUTES'
    )
    result = []
    for page in pages:
        for item in page['Items']:
            result.append(item_to_hash(item=item))
    return jsonify(result)

@app.route("/users/<string:user_id>", methods=["GET"])
def get_user(user_id):
    resp = db.get_item(
        TableName=USERS_TABLE,
        Key={
            'userId':{'S': user_id}
        }
    )
    item = resp.get('Item')
    if not item:
        return jsonify({'error': 'User does not exist'}), 404

    return jsonify(item_to_hash(item=item))

@app.route("/users", methods=["POST"])
def create_user():
    user_id = request.json.get('userId')
    name = request.json.get('name')

    if not user_id or not name:
        return jsonify({'error': 'Please provide userId or name'}), 400

    db.put_item(
        TableName=USERS_TABLE,
        Item={
            'userId': {'S': user_id},
            'name': {'S': name}
        }
    )

    return jsonify({
        'userId': user_id,
        'name': name
    })


@app.route("/users/<string:user_id>", methods=["PUT"])
def edit_user(user_id):
    resp = db.get_item(
        TableName=USERS_TABLE,
        Key={
            'userId':{'S': user_id}
        }
    )
    item = resp.get('Item')
    if not item:
        return jsonify({'error': 'User does not exist'}), 404

    name = request.json.get('name')
    db.put_item(
        TableName=USERS_TABLE,
        Item={
            'userId': {'S': user_id},
            'name': {'S': name}
        }
    )

    return jsonify({
        'userId': user_id,
        'name': name
    })

@app.route("/users/<string:user_id>", methods=["DELETE"])
def remove_user(user_id):
    resp = db.get_item(
        TableName=USERS_TABLE,
        Key={
            'userId':{'S': user_id}
        }
    )
    item = resp.get('Item')
    if not item:
        return jsonify({'error': 'User does not exist'}), 404

    result = item_to_hash(item=item)
    db.delete_item(
        TableName=USERS_TABLE,
        Key={
            'userId': {
                'S': user_id
            }
        }
    )

    return jsonify(result)