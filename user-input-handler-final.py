import json
import logging
import boto3
from datetime import datetime

def lambda_handler(event, context):

  logger = logging.getLogger()
  logger.setLevel(logging.INFO)

  # Get user input from API Gateway event and perform basic validation
  try:
    user_input = event['body']
    if not user_input.strip():
      raise ValueError("Empty user input")
  except (KeyError, ValueError) as e:
    logger.error(f"Invalid user input: {str(e)}")
    return {
      'statusCode': 400,
      'body': json.dumps('Invalid user input provided')
    }

  # Craft a Comprehensive Prompt Considering User Input and Context
  delight = "placeholder"
  prompt = f"The user asks: {user_input}. Considering the user's loyalty program details "  # Base prompt

  # Access user context (e.g., recent purchases, program details) based on data source (replace with actual logic)
  user_context = retrieve_user_context(event)
  prompt += f"({user_context.get('program_details', '{}')}), "  # Include program details

  # Integrate optional Bedrock Knowledge Base snippet for additional information
  try:
    bedrock_client = boto3.client('bedrock')
    knowledge_base_name = "your_bedrock_knowledge_base"
    response = bedrock_client.search(
      KnowledgeBaseArn=f"arn:aws:bedrock:{your_aws_region}:{your_account_id}:knowledge-base/{knowledge_base_name}",
      Query=user_input,
    )
    knowledge_base_snippet = response['Results'][0]['Content']  # Replace with actual data retrieval logic
    prompt += f"and potentially relevant information from the knowledge base ({knowledge_base_snippet}) "
  except Exception as e:
    logger.error(f"Error retrieving information from Bedrock: {str(e)}")

  # Leverage Claude v2 to Generate Insights within Loyalty Program Context
  try:
    bedrock_client = boto3.client('bedrock')
    model_id = "your_bedrock_model_id"  # Replace with your actual model ID
    response = bedrock_client.invoke_model(
      Body=json.dumps({"inputText": prompt}),
      ModelId=model_id
    )
    bedrock_response = response['body'].read().decode('utf-8')
  except Exception as e:
    logger.error(f"Error calling Bedrock: {str(e)}")
    return {
      'statusCode': 500,
      'body': json.dumps(f"Error during Bedrock analysis: {str(e)}")
    }

  # Process Claude v2 Response and Generate Personalized Loyalty Insights
  processed_response = generate_loyalty_insights(user_input, bedrock_response, user_context)

  # Amazon Kendra Integration 
  kendra_client = boto3.client('kendra')
  s3_bucket_name = "your-s3-bucket-name"  

  if processed_response and user_context.get("insights_action") == "find_offers":
    # Check if scraped data is available in S3 (avoid redundant scraping)
    if not is_data_available_in_s3(s3_bucket_name):
      marketplace_url = "https://mock-marketplace.com"  # Replace with URL
      scrape_and_store_data_in_s3(kendra_client, marketplace_url, s3_bucket_name)

    # Extract relevant keywords from processed_response for offer matching
    keywords = extract_keywords_for_offer_matching(processed_response)
    if keywords:
      matched_offers = find_offers_using_kendra(keywords, s3_bucket_name)
      processed_response += f"\nAdditionally, I found some offers picked for you on marketplace:\n {matched_offers}"

  # Return JSON Response with relevant data
  return {
    'statusCode': 200,
    'body': json.dumps({
      "user_input": user_input,
      "processed_response": processed_response,
      "knowledge_base_snippet": knowledge_base_snippet if knowledge_base_snippet else None,
      "timestamp": datetime.utcnow().isoformat()
    })
  }


# Function to retrieve user context based on data source (replace with actual implementation)
def retrieve_user_context(event):
  # Implement logic to access user data (e.g., from DynamoDB, user profile)
  # based on information provided in the API Gateway event
  return {
    'program_details': {'name': 'Rewards Program X', 'points_balance': 1000}  # Example program details
  }


# Function to generate personalized loyalty insights (replace with tailored logic)
def generate_loyalty_insights(user_input, bedrock_response, user_context):

  insights = []

  # Analyze Response to Extract Insights
  try:
    # Replace with logic to parse Claude v2 response (potentially JSON format)
    claude_data = json.loads(bedrock_response)
    extracted_data = claude_data.get('data', {})  # Assuming 'data' key holds extracted information

    # Example: Identify spending trends and suggest relevant rewards
    recent_purchases = extracted_data.get('recent_purchases', [])
    program_details = user_context.get('program_details', {})
    points_balance = program_details.get('points_balance', 0)

    if recent_purchases:
      # Analyze spending patterns (e.g., most frequent categories)
      spending_categories = [item['category'] for item in recent_purchases]
      frequent_categories = max(set(spending_categories), key=spending_categories.count)

      # Suggest relevant rewards based on spending patterns and points balance
      potential_rewards = claude_data.get('potential_rewards', [])  # Assuming 'potential_rewards' key exists
      relevant_rewards = [reward for reward in potential_rewards if reward['category'] in frequent_categories and reward['points_cost'] <= points_balance]

      if relevant_rewards:
        insights.append(f"Based on your recent purchases in {frequent_categories}, you might be interested in these rewards to maximize your points:")
        for reward in relevant_rewards:
          insights.append(f"- {reward['name']} (Cost: {reward['points_cost']} points)")
      else:
        insights.append(f"You don't seem to have many recent purchases in a specific category. Explore our rewards catalog to find something you like!")

  except Exception as e:
    logger.error(f"Error parsing Claude v2 response: {str(e)}")
    insights.append("An error occurred while generating insights. Please try again later.")

  # Return a list of personalized loyalty program insights
  return insights


#Another function to generate rewards insights 
def generate_loyalty_insights(user_input, bedrock_response, user_context):

  insights = []

  # Analyze Claude v2 Response to Extract Insights
  try:
    # Replace with logic to parse Claude v2 response (potentially JSON format)
    # and extract relevant information for generating insights
    claude_data = json.loads(bedrock_response)
    extracted_data = claude_data.get('data', {})  # Assuming 'data' key holds extracted information

    # Example: Identify spending trends and suggest relevant rewards
    recent_purchases = extracted_data.get('recent_purchases', [])
    program_details = user_context.get('program_details', {})
    points_balance = program_details.get('points_balance', 0)

    if recent_purchases:
      # Analyze spending patterns (e.g., most frequent categories)
      spending_categories = [item['category'] for item in recent_purchases]
      frequent_categories = max(set(spending_categories), key=spending_categories.count)

      # Suggest relevant rewards based on spending patterns and points balance
      potential_rewards = claude_data.get('potential_rewards', [])  # Assuming 'potential_rewards' key exists
      relevant_rewards = [reward for reward in potential_rewards if reward['category'] in frequent_categories and reward['points_cost'] <= points_balance]

      if relevant_rewards:
        insights.append(f"Based on your recent purchases in {frequent_categories}, you might be interested in these rewards to maximize your points:")
        for reward in relevant_rewards:
          insights.append(f"- {reward['name']} (Cost: {reward['points_cost']} points)")
      else:
        insights.append(f"You don't seem to have many recent purchases in a specific category. Explore our rewards catalog to find something you like!")

  except Exception as e:
    logger.error(f"Error parsing Claude v2 response: {str(e)}")
    insights.append("An error occurred while generating insights. Please try again later.")

  # Return a list of personalized loyalty program insights
  return insights

