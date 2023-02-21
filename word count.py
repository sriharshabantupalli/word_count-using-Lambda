import boto3

s3 = boto3.client('s3')
dynamodb = boto3.client('dynamodb')


def handler(event, context):
    # Get the S3 bucket and key from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    # Get the contents of the S3 file
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')

    # Count the words in the file
    tokens = content.lower().split()
    word_count = len(tokens)
    word_freq = {}
    for token in tokens:
        if token in word_freq:
            word_freq[token] += 1
        else:
            word_freq[token] = 1

    # Convert the word frequency to a string
    word_freq_str = ''
    for word, freq in word_freq.items():
        word_freq_str += f'{word}:{freq},'
    word_freq_str = word_freq_str[:-1]  # remove the last comma

    # Save the results to DynamoDB
    item = {
        'file_name': {'S': key},
        'word_count': {'N': str(word_count)},
        'word_freq': {'S': word_freq_str}
    }
    dynamodb.put_item(TableName='word_counts', Item=item)

    return {
        'statusCode': 200,
        'body': 'Word count and frequency saved to DynamoDB'
    }