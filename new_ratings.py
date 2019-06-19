import pandas as pd
from sql_cnxn import redshift_cnxn

def to_int(df):
    cols = ['tp_job_id']
    for col in cols:
        df[col] = df[col].apply(lambda x: int(x) if x == x else "")

historic_df = pd.read_csv(r'/Volumes/Shared/Vetting and Quality/Business Validation/Negative Ratings/cleaned_negative_ratings_historic.csv'
                          , delimiter= ',', header=0,
                       parse_dates=['date_posted'], encoding='latin1')

new_ratings_df = pd.read_sql(r"""select tm_user_id as tp_user_id,
ho_user_id,
job_id + 12480 as job_ref,
overall_rating,
comment,
date_posted,
t.date_first_active_tm as first_active_date,
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
) as t1
inner join wh_tradesman t on t1.tm_user_id = t.user_id
where row_n = 1
order by date_posted""", redshift_cnxn, parse_dates=['date_posted'])

historic_df['tp_job_id'] = historic_df['tp_user_id'].astype(str) + historic_df['job_ref'].astype(str)

to_int(historic_df)

new_ratings_df['tp_job_id'] = new_ratings_df['tp_user_id'].astype(str) + new_ratings_df['job_ref'].astype(str)

to_int(new_ratings_df)

ratings_de_dupe_list = historic_df['tp_job_id'].to_list()

remove_flag = []

for row in new_ratings_df['tp_job_id']:
    if row in ratings_de_dupe_list:
        remove_flag.append("y")
    else:
        remove_flag.append("n")

new_ratings_df['remove_flag'] = remove_flag

new_ratings_df = new_ratings_df[new_ratings_df.remove_flag != 'y']

new_ratings_df = new_ratings_df.drop(['remove_flag'], axis=1)

new_ratings_df.to_csv(r'/Volumes/Shared/Vetting and Quality/Business Validation/Negative Ratings/negative_ratings.csv',
                      index=False, sep=',', encoding='utf-8')


