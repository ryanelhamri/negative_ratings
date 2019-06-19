import pandas as pd
import runpy
from sql_cnxn import redshift_cnxn

def to_int(df):
    cols = ['tp_job_id']
    for col in cols:
        df[col] = df[col].apply(lambda x: int(x) if x == x else "")

input_df = pd.read_csv(r'/Volumes/Shared/Vetting and Quality/Business Validation/Negative Ratings/cleaned_negative_ratings_historic.csv'
                       , delimiter= ',', header=0,
                       parse_dates=['date_posted'])

prev_negative_ratings_df = pd.read_csv(
    r'/Volumes/Shared/Vetting and Quality/Business Validation/Negative Ratings/negative_ratings.csv',
    delimiter= ',', header=0, parse_dates=['date_posted'])

prev_negative_ratings_df['tp_job_id'] = prev_negative_ratings_df['tp_user_id'].astype(str) \
                                        + prev_negative_ratings_df['job_ref'].astype(str)

to_int(prev_negative_ratings_df)

input_df['tp_job_id'] = input_df['tp_user_id'].astype(str) \
                                        + input_df['job_ref'].astype(str)

to_int(input_df)

y_ratings = prev_negative_ratings_df[prev_negative_ratings_df['vq_check'] == 'y']

to_strike_pol = []

y_ratings_to_strike = y_ratings['tp_job_id'].tolist()

if prev_negative_ratings_df['vq_check'].hasnans == False:
    for row in prev_negative_ratings_df['tp_job_id']:
        if row in y_ratings_to_strike:
            to_strike_pol.append('y')
        else:
            to_strike_pol.append('n')
    prev_negative_ratings_df['rating_to_strike_pol'] = to_strike_pol
else:
    pass

strike_pol_df = prev_negative_ratings_df[prev_negative_ratings_df.rating_to_strike_pol == 'y']

strike_pol_df.to_csv(r'strike_policy_df.csv', index=False, sep=',')

# find unique job+tp ids, then create list of these, do the same for historic list
# for row in historic if in y_ratings list then assign y if not then n
# add above logic to strike_policy to find relevant jobs to be run through policy and filtered down to case_generator

if prev_negative_ratings_df['vq_check'].hasnans == False:
    runpy.run_path(
        r"""/Users/ryan.elhamri/OneDrive - Rated People Ltd/Documents/Python/negative_ratings/strike_policy.py""")
    runpy.run_path(
        r"""/Users/ryan.elhamri/OneDrive - Rated People Ltd/Documents/Python/negative_ratings/new_ratings.py""")
    input_df.append(y_ratings, sort=True)
    input_df.to_csv(r"""cleaned_negative_ratings_historic_test.csv""", sep=',', index=False)
else:
    new_ratings_df = pd.read_sql(r"""select tm_user_id as tp_user_id,
    ho_user_id,
    job_id + 12480 as job_ref,
    overall_rating,
    comment,
    date_posted,
    '' as vq_check
    from
    	(
    	select tm_user_id,
    	ho_user_id,
    	job_id,
    	overall_rating,
    	comment,
    	date_posted,
    	row_number() over(partition by tm_job_id order by date_posted asc) as row_n
    	from 
    		(
    		select tm_user_id,
    		ho_user_id,
    		job_id,
    		overall_rating,
    		comment,
    		concat(tm_user_id,job_id) as tm_job_id,
    		created_ts as date_posted
    		from wh_ratings r
    		where date_posted >= case
                                 when extract(dow
                                              from current_date) = 1 then dateadd(day,-3,current_date)
                                 else dateadd(day,-1,current_date)
                             end
         	and date_posted <> current_date
    		and overall_rating <= 6)
    )
    where row_n = 1""", redshift_cnxn, parse_dates=['date_posted'])
    prev_negative_ratings_df.append(new_ratings_df)
    prev_negative_ratings_df.to_csv(r"""/Volumes/Shared/Vetting and Quality/Business Validation/Negative Ratings/negative_ratings.csv""",
                                    sep=',', index=False, encoding= 'utf-8')