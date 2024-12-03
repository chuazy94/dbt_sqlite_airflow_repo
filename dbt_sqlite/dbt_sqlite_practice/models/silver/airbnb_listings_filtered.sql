{{ config(materialized='table') }}


select
id
, last_scraped
, host_id
, host_name
, host_since
, host_location
, host_about
, host_response_time
, host_listings_count
, review_scores_rating
, review_scores_value
, review_scores_location
, review_scores_accuracy
, review_scores_checkin
, review_scores_cleanliness
, review_scores_communication
, last_review
, last_scraped
, neighbourhood
, neighbourhood_cleansed
from
 {{ source('raw_data', 'listings') }}