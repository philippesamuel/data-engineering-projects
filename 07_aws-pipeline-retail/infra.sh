# Create a bucket
aws s3 mb s3://${S3_BUCKET_NAME} --region ${AWS_REGION}

# upload data to bucket
aws s3 cp ./data/raw/online_retail.csv s3://${S3_BUCKET_NAME}/raw/


# create database to hold data
aws glue create-database --cli-input-json "$(envsubst < infra_glue_db_raw.json)"
aws glue create-table --cli-input-json "$(envsubst < infra_glue_table_curated.json)"


# create iam role to allow crawler access to the bucket
aws iam create-role \
    --role-name "AWSGlueServiceRole-onlineRetailCrawler" \
    --assume-role-policy-document file://glue-trust-policy.json > glue-crawler-iam-role.json

aws iam attach-role-policy \
    --role-name "AWSGlueServiceRole-onlineRetailCrawler" \
    --policy-arn "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"

aws iam put-role-policy \
    --role-name "AWSGlueServiceRole-onlineRetailCrawler" \
    --policy-name "CrawlerS3Access" \
    --policy-document file://s3-access-policy.json

# wait for role creation to propagate
sleep 10

# create crawler to infer schema
aws glue create-crawler \
    --name "online_retail_crawler" \
    --role $(jq -r .Role.Arn glue-crawler-iam-role.json) \
    --database-name "${GLUE_DB_NAME}" \
    --targets "{\"S3Targets\": [{\"Path\": \"s3://${S3_BUCKET_NAME}/raw/\"}]}" \
    --description "Crawler for raw S3 online retail data"

# run crawler to infer schema and create table
aws glue start-crawler --name "online_retail_crawler" 


echo "Waiting for crawler to complete..."
while true; do
    STATE=$(aws glue get-crawler --name "online_retail_crawler" --query 'Crawler.State' --output text)
    if [[ "$STATE" == "READY" ]]; then
        echo "Crawler finished."
        break
    else
        echo "Crawler is currently: $STATE. Waiting 10 seconds..."
        sleep 10
    fi
done


# infer schema and register metadata in Glue Data Catalog
aws glue create-table  