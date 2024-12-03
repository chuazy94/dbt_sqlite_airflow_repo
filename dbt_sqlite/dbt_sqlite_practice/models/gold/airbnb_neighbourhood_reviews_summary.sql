{{ config(materialized='view') }}

select
neighbourhood_cleansed
, avg(review_scores_rating)
, avg(review_scores_value)
, avg(review_scores_location)
, avg(review_scores_accuracy)
, avg(review_scores_checkin)
, avg(review_scores_cleanliness)
, avg(review_scores_communication)
from
{{ ref('airbnb_listings_filtered') }}
group by neighbourhood_cleansed
order by avg(review_scores_rating) desc