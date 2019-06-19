import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta
from sql_cnxn import redshift_cnxn

def merge_df_left(df1, df2):
    var_df = pd.merge(df1, df2, how='left')
    return var_df

def merge_df_inner(df1, df2):
    var_df = pd.merge(df1, df2, how='inner')
    return var_df

def to_int(df):
    cols = ['tp_user_id']
    for col in cols:
        df[col] = df[col].apply(lambda x: int(x))

def to_int2(df):
    cols = ['tp_job_id']
    for col in cols:
        df[col] = df[col].apply(lambda x: int(x) if x == x else "")

spend_percentile = 98

one_year_ago = date.today() - relativedelta(years=1)

three_months_ago = date.today() - relativedelta(days=90)

one_year_ago = pd.to_datetime(one_year_ago)

historic_df = pd.read_csv(r'/Volumes/Shared/Vetting and Quality/Business Validation/Negative Ratings/cleaned_negative_ratings_historic.csv'
                          , delimiter=',', header=0,
                          parse_dates=['date_posted'])

strike_policy_df = pd.read_csv(r'strike_policy_df.csv', delimiter=',', header=0,
                               parse_dates=['date_posted', 'first_active_date'])

to_int(strike_policy_df)

historic_df = historic_df[(historic_df['date_posted'] >= one_year_ago)]

historic_df['strike_rate'] = historic_df.groupby('tp_user_id')['tp_user_id'].transform('count')

strike_policy_df['end_of_first_ninety'] = strike_policy_df['first_active_date'] + pd.DateOffset(days=90)

historic_df['tp_job_id'] = historic_df['tp_user_id'].astype(str) \
                                        + historic_df['job_ref'].astype(str)

to_int2(historic_df)

spend_data = pd.read_sql(f"""select buckets, max(total_spend) as total_spend
from(
select total_spend,
		ntile(100) over (order by total_spend) as buckets
		from (select tradesman_user_id,
				sum(price) as total_spend
				from wh_purchased_jobs
				group by 1)
		where total_spend <> 0
)
where buckets = {spend_percentile}
group by 1""", redshift_cnxn)

strike_tp_lists = strike_policy_df['tp_user_id'].tolist()

strike_tp_lists = tuple(strike_tp_lists)

pat_flag = pd.read_sql(f"""select user_id as tp_user_id, pat
from(
select tt.user_id, tt.current_plan_code, dp.pat, dd.date,
row_number() over(partition by tt.user_id order by dd.date) as row_n
from wh_tradesman_tracker tt
inner join dim_date dd on tt.date_id = dd.id
inner join dim_plan dp on tt.current_plan_code = dp.plan_code
where tt.rp_user_status_id = 1
and tt.user_id in {strike_tp_lists}
)
where row_n = 1
and pat = 'Y'""", redshift_cnxn)

total_spend_df = pd.read_sql(f"""select tradesman_user_id as tp_user_id,
				sum(price) as total_spend
				from wh_purchased_jobs
				group by 1""", redshift_cnxn)

historic_df['tp_user_id'] = historic_df['tp_user_id'].astype(int)

pat_flag['tp_user_id'] = pat_flag['tp_user_id'].astype(int)

strike_policy_df = merge_df_left(strike_policy_df, pat_flag)

total_spend_df['tp_user_id'] = total_spend_df['tp_user_id'].astype(int)

strike_policy_df = merge_df_inner(strike_policy_df, total_spend_df)

high_spender_num = spend_data['total_spend'].tolist()

high_spender_flag = []

for row in strike_policy_df.total_spend:
    if row >= high_spender_num[0]:
        high_spender_flag.append('y')
    else:
        high_spender_flag.append('n')

strike_policy_df['high_spender_flag'] = high_spender_flag

strike_count_df = historic_df[['tp_user_id', 'strike_rate']]

strike_policy_df = pd.merge(strike_policy_df, strike_count_df, how='inner', on='tp_user_id')

blacklist_df1 = strike_policy_df[((strike_policy_df['strike_rate'] == 5) &
                                (strike_policy_df['pat'] == 'y'))  &
                                ((strike_policy_df['high_spender_flag'] == 'y') &
                                 (strike_policy_df['strike_rate'] == 5))]

blacklist_df2 = strike_policy_df[((strike_policy_df['strike_rate'] == 3) & (strike_policy_df['pat'] != 'Y') &
                                (strike_policy_df['high_spender_flag'] == 'n'))]

blacklist_df3 = strike_policy_df[((strike_policy_df['strike_rate'] == 2) &
                                (strike_policy_df['end_of_first_ninety'] >= date.today()))]

blacklist_df = blacklist_df1.append([blacklist_df2,blacklist_df3])

to_int2(blacklist_df)

blacklist_df = blacklist_df.drop_duplicates(subset= 'tp_job_id')

blacklist_df.to_csv(r'/Volumes/Shared/Vetting and Quality/Business Validation/Negative Ratings/to_be_blacklist.csv',
                    index=False, sep=',')

blacklist_df_id_list = blacklist_df['tp_job_id'].tolist()

to_investigate = []

for row in strike_policy_df.tp_job_id:
    if row in blacklist_df_id_list:
        to_investigate.append("y")
    else:
        to_investigate.append("n")

strike_policy_df['to_investigate'] = to_investigate

to_investigate_df = strike_policy_df[(strike_policy_df['to_investigate'] == 'y')]

to_investigate_df.to_csv(r'/Volumes/Shared/Vetting and Quality/Business Validation/Negative Ratings/to_investigate_df.csv',
                         index=False, sep=',')